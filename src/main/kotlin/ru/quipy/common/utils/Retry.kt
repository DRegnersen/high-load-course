package ru.quipy.common.utils

import kotlinx.coroutines.delay
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.logger
import ru.quipy.payments.logic.RetryPolicy
import ru.quipy.payments.logic.now
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

fun <T> retryAsync(
    policy: RetryPolicy,
    deadlineMillis: Long,
    shouldRetry: (T) -> Boolean,
    block: () -> CompletableFuture<T>
): CompletableFuture<T> {
    return retryAsync(
        maxAttempts = policy.maxAttempts,
        initialDelayMillis = policy.initialDelay.toMillis(),
        maxDelayMillis = policy.maxDelay.toMillis(),
        factor = policy.factor,
        deadlineMillis,
        shouldRetry,
        block
    )
}

fun <T> retryAsync(
    maxAttempts: Int = 2,
    initialDelayMillis: Long = 100,
    maxDelayMillis: Long = 1000,
    factor: Double = 1.0,
    deadlineMillis: Long,
    shouldRetry: (T) -> Boolean,
    block: () -> CompletableFuture<T>
): CompletableFuture<T> {
    val scheduler = Executors.newSingleThreadScheduledExecutor()

    fun attempt(attemptNumber: Int, delay: Long): CompletableFuture<T> {
        if (now() > deadlineMillis) {
            logger.warn("Deadline exceeded, stopping retries.")
            return block()
        }

        val resultFuture = CompletableFuture<T>()

        val runAttempt = {
            block().whenComplete { result, error ->
                if (error != null) {
                    resultFuture.completeExceptionally(error)
                } else if (shouldRetry(result) && attemptNumber < maxAttempts) {
                    logger.warn("Attempt $attemptNumber failed, retrying in $delay ms...")

                    val nextDelay = (delay * factor).toLong().coerceAtMost(maxDelayMillis)

                    attempt(attemptNumber + 1, nextDelay).whenComplete { finalResult, finalError ->
                        if (finalError != null) {
                            resultFuture.completeExceptionally(finalError)
                        } else {
                            resultFuture.complete(finalResult)
                        }
                    }
                } else {
                    resultFuture.complete(result)
                }
            }
        }

        if (attemptNumber == 1) {
            runAttempt.invoke()
        } else {
            scheduler.schedule(runAttempt, delay, TimeUnit.MILLISECONDS)
        }

        return resultFuture
    }

    return attempt(attemptNumber = 1, delay = initialDelayMillis)
}

suspend fun <T> retrySuspend(
    policy: RetryPolicy,
    deadlineMillis: Long,
    shouldRetry: (T) -> Boolean,
    block: suspend () -> T
): T {
    return retrySuspend(
        maxAttempts = policy.maxAttempts,
        initialDelayMillis = policy.initialDelay.toMillis(),
        maxDelayMillis = policy.maxDelay.toMillis(),
        factor = policy.factor,
        deadlineMillis,
        shouldRetry,
        block
    )
}

suspend fun <T> retrySuspend(
    maxAttempts: Int = 2,
    initialDelayMillis: Long = 100,
    maxDelayMillis: Long = 1000,
    factor: Double = 1.0,
    deadlineMillis: Long,
    shouldRetry: (T) -> Boolean,
    block: suspend () -> T
): T {
    var currentDelay = initialDelayMillis

    repeat(maxAttempts - 1) { attempt ->
        val result = block()

        if (!shouldRetry(result))
            return result

        if (now() + currentDelay > deadlineMillis) {
            logger.warn("Deadline exceeded, stopping retries.")
            return result
        }

        logger.warn("Attempt ${attempt + 1} failed, retrying in $currentDelay ms...")

        delay(currentDelay)
        currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelayMillis)
    }

    return block() // last attempt
}