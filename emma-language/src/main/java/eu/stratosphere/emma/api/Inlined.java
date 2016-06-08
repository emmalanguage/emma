package eu.stratosphere.emma.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Runtime annotation indicating that the annotated method
 * has been prepared for inlining.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Inlined {
    /**
     * @return the name of the inlineable variant (in the same scope)
     */
    String forwardTo();
}
