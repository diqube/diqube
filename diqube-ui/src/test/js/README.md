#JavaScript tests#

This directory contains JavaScript unit tests. Each test file (which has to be named with the suffix ".test.js") will 
actually be interpreted twice by the build system:

1. From a standard Java TestNG test, whose class is defined dynamically by `JavaScriptDataValidationTestFactory`.
2. By Karma which will execute normal Jasmine logic.

For the latter there seems to be no explanation needed. The first run, though, might need some exaplanation:

##Data type check##

The JavaScript code will at runtime evaluate (JavaScript) objects it received from the diqube-ui server-side via a 
WebSocket: The JavaScript sends requests through the socket to the server and the server will resond with corresponding
result objects.
As JavaScript does not provide any compile-time verification that the objects it will receive at runtime have the 
properties the code expects, we provide the data type check test here.

The objects that the server sends to the browser through the WebSocket all are implementations of `JsonResult` and
objects of these can be validated. That is what the data type check ought to do: It validates if the JavaScript objects,
a test wants to use on the JavaScript side of things would be a valid object that the server might actually return at
runtime. We do this with a small code snippet that is used in some form in each test:

```
 var dataObj = validatedData.data("version", { 
   gitCommitLong: "abcdef",
   gitCommitShort: "abc",
   buildTimestamp: "today"
 });
```

It accesses `validatedData` which will be bound to different objects in the two executions of the test.js file:
* In the data type check, `validatedData` will be bound to a Java class that actually validates if the object given
  is a valid object of type "version" (which would resolve to `VersionJsonResult` class).
* In the Karma/Jasmine execution, this will be bound to an object that will simply return the given object on a call to
  `data(..)`. Be sure to use `dataObj` to reference the actual test object throughout the rest of your test file.

The data type check test will ignore everything else that is available in the .test.js file, but only inspect the calls
to `validatedData`. 