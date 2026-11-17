package io.github.satyaprakashdwivedi.streaming;

/** Any configuration or runtime problem in a pipeline. Messages name the step and option involved. */
public class PipelineException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PipelineException(String message) {
        super(message);
    }

    public PipelineException(String message, Throwable cause) {
        super(message, cause);
    }
}
