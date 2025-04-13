package ru.quipy.common.utils

import kotlinx.coroutines.delay
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.logger
import ru.quipy.payments.logic.RetryPolicy
import ru.quipy.payments.logic.now


suspend fun <T> retry(
    policy: RetryPolicy,
    deadlineMillis: Long,
    shouldRetry: (T) -> Boolean,
    block: suspend () -> T
): T {
    return retry(
        maxAttempts = policy.maxAttempts,
        initialDelayMillis = policy.initialDelay.toMillis(),
        maxDelayMillis = policy.maxDelay.toMillis(),
        factor = policy.factor,
        deadlineMillis,
        shouldRetry,
        block
    )
}

suspend fun <T> retry(
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
            logger.warn("Deadline exceeded, stopping retries")
            return result
        }

        logger.warn("Attempt ${attempt + 1} failed, retrying in $currentDelay ms...")

        delay(currentDelay)
        currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelayMillis)
    }

    return block() // last attempt
}