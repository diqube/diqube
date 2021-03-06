/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.data.types.dbl.dict;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SDoubleDictionaryFpcPage;
import org.diqube.data.serialize.thrift.v1.SDoubleDictionaryFpcState;
import org.diqube.util.DoubleUtil;
import org.diqube.util.Holder;
import org.diqube.util.Pair;

/**
 * Contains a sub-set of double values of a {@link FpcDoubleDictionary} in a compressed form.
 * 
 * <p>
 * The values are compressed with an algorithm that is inspired by the "FPC" algorithm by Martin Burtscher and Paruj
 * Ratanaworabhan from the paper "High Throughput Compression of Double-Precision Floating-Point Data".
 * 
 * <p>
 * Decompressing values only happens in a linear way, i.e. if the double value at index i is to be decoded, all double
 * values with index < i have to be decoded, too. Therefore the compressed values are split up into pages - each page
 * contains the state of the compression algorithm so it can start decompressing linearily at the first value that is
 * stored in the page - in contrast to storing all double values in a single page where we'd always need to decompress
 * /all/ previous values.
 * 
 * <p>
 * After instantiating a new instance, call {@link #compress(double[])} to fill it with values.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SDoubleDictionaryFpcPage.class)
public class FpcPage implements DataSerialization<SDoubleDictionaryFpcPage>, Iterable<Pair<Long, Double>> {
  public static final int HASH_TABLE_SIZE = 16; // 1 kB, needs to be a power of 2.

  private static final byte FCM = 0;
  private static final byte DFCM = 1;

  private byte[] data;

  private State startState;

  private int size;

  private long firstId;

  /** for deserialization */
  public FpcPage() {

  }

  /**
   * Set up a new page as a follow-up one of another one that has been {@link #compress(double[])}ed - the state
   * parameter is the result of the compress method of the previous page.
   */
  public FpcPage(long firstId, State startState) {
    this.firstId = firstId;
    this.startState = startState;
  }

  /**
   * Create a new Page without a start state (this is used for the first page only!)
   */
  public FpcPage(long firstId) {
    this.firstId = firstId;
    this.startState = null;
  }

  /**
   * Decompress value at a specific index.
   * 
   * Please note that all values with indices < the given index have to be decompressed internally, too.
   * 
   * @throws ArrayIndexOutOfBoundsException
   *           if index not available.
   */
  public double get(int index) throws ArrayIndexOutOfBoundsException {
    return get(index, index)[0];
  }

  /**
   * @return Number of double values compressed here.
   */
  public int getSize() {
    return size;
  }

  /**
   * Decompress values of a range of indices.
   * 
   * Please note that all values with indices < the given startIndexInclusive have to be decompressed internally, too.
   * 
   * @throws ArrayIndexOutOfBoundsException
   *           if index not available.
   */
  public double[] get(int startIndexInclusive, int endIndexInclusive) throws ArrayIndexOutOfBoundsException {
    if (endIndexInclusive >= size || startIndexInclusive < 0 || startIndexInclusive > endIndexInclusive)
      throw new ArrayIndexOutOfBoundsException("Tried to access index " + startIndexInclusive + "-" + endIndexInclusive
          + " but there are only " + size + " entries.");

    double[] res = new double[endIndexInclusive - startIndexInclusive + 1];

    decompress(new DecompressCallback() {
      @Override
      public boolean decompressed(int idx, double value) {
        if (idx >= startIndexInclusive)
          res[idx - startIndexInclusive] = value;

        return idx < endIndexInclusive;
      }
    });

    return res;
  }

  /**
   * Searches linearily for the given value in this page.
   * 
   * @param value
   *          The value to be searched
   * @return Either a positive (>= 0) value in which case the value is the ID of the entry whose value is equal to the
   *         given value. If the result is negative, it is encoded as -(id + 1) where ID is the insertion point of the
   *         value (= ID of next bigger value).
   */
  public int findIndex(double value) {
    int[] res = new int[1];
    res[0] = -(size + 1);
    decompress(new DecompressCallback() {
      @Override
      public boolean decompressed(int curIdx, double curValue) {
        double parentVal = new Double(value);
        if (DoubleUtil.equals(curValue, parentVal)) {
          res[0] = curIdx;
          return false;
        }
        if (curValue > value) {
          res[0] = -(curIdx + 1);
          return false;
        }
        return true;
      }
    });

    return res[0];
  }

  /**
   * @return The logical ID of the first entry in this page.
   */
  public long getFirstId() {
    return firstId;
  }

  /**
   * Decompress all values.
   * 
   * @param callback
   *          When a new value has been decompressed the callback is called and the caller can act accordingly.
   */
  private void decompress(DecompressCallback callback) {
    State curState = createInitialDecompressState();

    boolean continueProcessing = true;
    Holder<Integer> dataPos = new Holder<Integer>(0);
    int curIndex = 0;
    do {
      double value = decompressNext(dataPos, curState);

      continueProcessing = callback.decompressed(curIndex++, value);
    } while (continueProcessing && curIndex < size);
  }

  /**
   * Decompress a value at a specific location and move forward.
   * 
   * @param dataPosHolder
   *          The holder of the position in {@link #data} where the to-be-read entry starts. This holder will be updated
   *          with the location of the next entry upon method return.
   * @param state
   *          The state that was valid at the end of the decompression of the previous value. If the first value in
   *          {@link #data} should be decompressed, pass in the result of {@link #createInitialDecompressState()}.
   * @return The decompressed double value.
   */
  private double decompressNext(Holder<Integer> dataPosHolder, State state) {
    int dataPos = dataPosHolder.getValue();
    byte mgmtByte = data[dataPos];
    byte numberOfLeadingZeroBytes = (byte) (mgmtByte & 7);
    byte numberOfRemainingBytes = (byte) (8 - numberOfLeadingZeroBytes);
    byte compressionMethod = (byte) ((mgmtByte & 8) >>> 3);

    long xoredValue = 0;
    for (int byteChunk = 0; byteChunk < numberOfRemainingBytes; byteChunk++)
      xoredValue |= (data[dataPos + 1 + byteChunk] & 0xffL) << (byteChunk * 8);

    long fcmPrediction = state.fcmHashTable[state.fcmHash];
    long dfcmPrediction = state.dfcmHashTable[state.dfcmHash] + state.lastValue;

    long value;
    if (compressionMethod == FCM)
      value = xoredValue ^ fcmPrediction;
    else
      value = xoredValue ^ dfcmPrediction;

    state.fcmHashTable[state.fcmHash] = value;
    state.fcmHash = (byte) (((((long) state.fcmHash) << 6) ^ (value >>> 48)) & (((long) HASH_TABLE_SIZE) - 1));

    state.dfcmHashTable[state.dfcmHash] = value - state.lastValue;
    state.dfcmHash =
        (byte) (((((long) state.dfcmHash) << 2) ^ ((value - state.lastValue) >>> 40)) & (((long) HASH_TABLE_SIZE) - 1));
    state.lastValue = value;

    dataPosHolder.setValue(dataPos + 1 + numberOfRemainingBytes);

    return Double.longBitsToDouble(state.lastValue);
  }

  private State createInitialDecompressState() {
    State state = new State();

    if (startState != null) {
      state.fcmHashTable = Arrays.copyOf(startState.fcmHashTable, startState.fcmHashTable.length);
      state.dfcmHashTable = Arrays.copyOf(startState.dfcmHashTable, startState.dfcmHashTable.length);
      state.fcmHash = startState.fcmHash;
      state.dfcmHash = startState.dfcmHash;
      state.lastValue = startState.lastValue;
    } else {
      state.fcmHashTable = new long[HASH_TABLE_SIZE];
      state.dfcmHashTable = new long[HASH_TABLE_SIZE];
      state.fcmHash = 0;
      state.dfcmHash = 0;
      state.lastValue = 0;
    }
    return state;
  }

  /**
   * Compress the given sorted values and store the results in this object.
   * 
   * @return The resulting {@link State} that can be used for the next state (the one that will compress the following
   *         values).
   */
  public State compress(double[] sortedValues) {
    long[] fcmHashTable;
    long[] dfcmHashTable;
    byte fcmHash;
    byte dfcmHash;
    long lastValue;

    if (startState != null) {
      fcmHashTable = Arrays.copyOf(startState.fcmHashTable, startState.fcmHashTable.length);
      dfcmHashTable = Arrays.copyOf(startState.dfcmHashTable, startState.dfcmHashTable.length);
      fcmHash = startState.fcmHash;
      dfcmHash = startState.dfcmHash;
      lastValue = startState.lastValue;
    } else {
      fcmHashTable = new long[HASH_TABLE_SIZE];
      dfcmHashTable = new long[HASH_TABLE_SIZE];
      fcmHash = 0;
      dfcmHash = 0;
      lastValue = 0;
    }

    List<Byte> temporaryRes = new ArrayList<>(sortedValues.length * 3);
    for (int i = 0; i < sortedValues.length; i++) {
      long value = Double.doubleToRawLongBits(sortedValues[i]);

      // fcm prediction
      long fcmPrediction = fcmHashTable[fcmHash];
      fcmHashTable[fcmHash] = value;
      fcmHash = (byte) (((((long) fcmHash) << 6) ^ (value >>> 48)) & (((long) HASH_TABLE_SIZE) - 1));

      // dfcm prediction
      long dfcmPrediction = dfcmHashTable[dfcmHash] + lastValue;
      dfcmHashTable[dfcmHash] = value - lastValue;
      dfcmHash = (byte) (((((long) dfcmHash) << 2) ^ ((value - lastValue) >>> 40)) & (((long) HASH_TABLE_SIZE) - 1));
      lastValue = value;

      if ((fcmPrediction ^ value) < (dfcmPrediction ^ value)) {
        // use fcmPrediction, because it has more leading zeros on xor than dfcmPrediction
        compressValue(fcmPrediction ^ value, FCM, temporaryRes);
      } else {
        // use dfcmPrediction
        compressValue(dfcmPrediction ^ value, DFCM, temporaryRes);
      }
    }

    size = sortedValues.length;
    data = new byte[temporaryRes.size()];
    for (int i = 0; i < data.length; i++)
      data[i] = temporaryRes.get(i);
    temporaryRes = null;

    State res = new State();
    res.fcmHashTable = fcmHashTable;
    res.fcmHash = fcmHash;
    res.dfcmHashTable = dfcmHashTable;
    res.dfcmHash = dfcmHash;
    res.lastValue = lastValue;
    return res;
  }

  private void compressValue(long xoredValue, byte compressionMethod, List<Byte> res) {
    byte numberOfLeadingZeroBytes = (byte) (Long.numberOfLeadingZeros(xoredValue) / 8);

    if (numberOfLeadingZeroBytes == 8)
      numberOfLeadingZeroBytes = 7;

    byte[] rest = new byte[8 - numberOfLeadingZeroBytes];
    for (int byteChunk = 0; byteChunk < rest.length; byteChunk++)
      rest[byteChunk] = (byte) (((xoredValue >>> (byteChunk * 8))) & 255);

    // TODO #6 this actually takes only 4 bit out of the byte
    byte mgmtByte = (byte) (numberOfLeadingZeroBytes | (compressionMethod << 3));

    res.add(mgmtByte);
    for (byte restByte : rest)
      res.add(restByte);
  }

  @Override
  public Iterator<Pair<Long, Double>> iterator() {
    return new Iterator<Pair<Long, Double>>() {
      private State decompressState = createInitialDecompressState();
      private Holder<Integer> nextDataPos = new Holder<>(0);
      private long numberDecompressed = 0L;

      @Override
      public boolean hasNext() {
        return numberDecompressed < size;
      }

      @Override
      public Pair<Long, Double> next() {
        double val = decompressNext(nextDataPos, decompressState);
        return new Pair<>(firstId + (numberDecompressed++), val);
      }
    };
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SDoubleDictionaryFpcPage target) throws SerializationException {
    target.setFirstId(firstId);
    target.setSize(size);
    target.setData(data);
    target.setStartState(mgr.serializeChild(SDoubleDictionaryFpcState.class, startState));
  }

  @Override
  public void deserialize(DataSerializationHelper mgr, SDoubleDictionaryFpcPage source)
      throws DeserializationException {
    firstId = source.getFirstId();
    size = source.getSize();
    data = source.getData();
    startState = mgr.deserializeChild(State.class, source.getStartState());
  }

  /**
   * @return approx number of bytes used by this page. Approx only!
   */
  public long calculateApproximateSizeInBytes() {
    long stateLength = 0L;
    if (startState != null)
      stateLength += 16 + // object header of State.
          startState.fcmHashTable.length * 4 + //
          startState.dfcmHashTable.length * 4 + //
          10;
    return 16 + // object header of this
        stateLength + data.length;
  }

  /**
   * Internally used callback interface for decompressing.
   */
  private static interface DecompressCallback {
    /**
     * A value at a specific index has been decompressed.
     * 
     * @return <code>true</code> if we should continue decompressing, <code>false</code> if decompressing should be
     *         stopped.
     */
    public boolean decompressed(int idx, double value);
  }

  /**
   * Identifies a state of the compression algorithm, which can be used to both, compress the next value(s) or
   * decompress the next value(s).
   */
  @DataSerializable(thriftClass = SDoubleDictionaryFpcState.class)
  public static class State implements DataSerialization<SDoubleDictionaryFpcState> {
    private long[] fcmHashTable;
    private long[] dfcmHashTable;
    private byte fcmHash; // byte is enough for up to 16kB of hash tables
    private byte dfcmHash;
    private long lastValue;

    @Override
    public void serialize(DataSerializationHelper mgr, SDoubleDictionaryFpcState target) throws SerializationException {
      target.setDfcmHash(dfcmHash);
      target.setFcmHash(fcmHash);
      target.setLastValue(lastValue);
      target.setFcmHashTable(LongStream.of(fcmHashTable).boxed().collect(Collectors.toList()));
      target.setDfcmHashTable(LongStream.of(dfcmHashTable).boxed().collect(Collectors.toList()));
    }

    @Override
    public void deserialize(DataSerializationHelper mgr, SDoubleDictionaryFpcState source)
        throws DeserializationException {
      fcmHash = source.getFcmHash();
      dfcmHash = source.getDfcmHash();
      lastValue = source.getLastValue();
      fcmHashTable = source.getFcmHashTable().stream().mapToLong(Long::longValue).toArray();
      dfcmHashTable = source.getDfcmHashTable().stream().mapToLong(Long::longValue).toArray();
    }
  }

}
