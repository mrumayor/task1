package krill.integration.exception;

public class KrillException extends RuntimeException {

    public KrillException(String s, Exception e)
    {
        super(s, e);
    }

    public KrillException(String s) {
        super(s);
    }
}
