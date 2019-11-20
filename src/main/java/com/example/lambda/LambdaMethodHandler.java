package com.example.lambda;

import com.amazonaws.services.lambda.runtime.Context;

/**
 * Simple example of Lambda.
 * Create new one, upload and set this class with path and method.
 * create test example with simple String "Example"
 * And press start test to see result.
 */
public class LambdaMethodHandler {
    public String handleRequest(String input, Context context) {
        context.getLogger().log("Input: " + input);
        return "Hello World - " + input;
    }
}
