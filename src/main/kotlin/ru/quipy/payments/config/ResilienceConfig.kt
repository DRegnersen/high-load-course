package ru.quipy.payments.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
@EnableConfigurationProperties(ResilienceProperties::class, RateLimiterProperties::class)
class ResilienceConfig

@ConstructorBinding
@ConfigurationProperties(prefix = "resilience")
data class ResilienceProperties(
    val requestTimeout: Long? = null,
    val connectTimeout: Long = 60000,
    val maxAttempts: Int = 1,
    val initialDelay: Long = 100,
    val maxDelay: Long = 30000,
    val delayFactor: Double = 1.0,
) {
    fun getRequestTimeoutDuration(): Duration? = requestTimeout?.let { Duration.ofMillis(it) }
    fun getConnectTimeoutDuration(): Duration = Duration.ofMillis(connectTimeout)
    fun getInitialDelayDuration(): Duration = Duration.ofMillis(initialDelay)
    fun getMaxDelayDuration(): Duration = Duration.ofMillis(maxDelay)
}

@ConstructorBinding
@ConfigurationProperties(prefix = "resilience.rate-limiter")
data class RateLimiterProperties(
    val window: Long = 1000,
) {
    fun getWindowDuration(): Duration = Duration.ofMillis(window)
}