package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final Object delegate;
  private final ZonedDateTime startTime;
  private final ProfilingState state;

  // TODO: You will need to add more instance fields and constructor arguments to this class.
  ProfilingMethodInterceptor(Clock clock, Object delegate, ZonedDateTime startTime, ProfilingState state) {
    this.clock = Objects.requireNonNull(clock);
    this.delegate = Objects.requireNonNull(delegate);
    this.startTime = Objects.requireNonNull(startTime);
    this.state = Objects.requireNonNull(state);

  }

    //public <T> ProfilingMethodInterceptor(Clock clock, T delegate, ZonedDateTime startTime, ProfilingState state) {

    //}
    private boolean isMethodProfiled(Method method) {
        return method.getAnnotation(Profiled.class) != null;
    }

    @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // TODO: This method interceptor should inspect the called method to see if it is a profiled
        //       method. For profiled methods, the interceptor should record the start time, then
        //       invoke the method using the object that is being profiled. Finally, for profiled
        //       methods, the interceptor should record how long the method call took, using the
        //       ProfilingState methods.
        Object result;
        Instant startT= null;
        boolean isProfiled = isMethodProfiled(method);

        if (isProfiled){
             startT = clock.instant();
        }
        try {
            result = method.invoke(delegate,args);
        }catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException ex) {
            throw ex.getTargetException();
        } finally {
            if (isProfiled){
                Duration duration = Duration.between(startT, clock.instant());
                state.record(delegate.getClass(), method, duration);
            }
        }
        return result;


    }
}
