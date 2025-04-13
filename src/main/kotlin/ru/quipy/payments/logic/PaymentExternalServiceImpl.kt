package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val resiliencePolicy: ResiliencePolicy,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    // Resilience policies
    private val timeoutPolicy = resiliencePolicy.timeout
    private val rateLimiterPolicy = resiliencePolicy.rateLimiter

    // Account properties
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .executor(Executors.newFixedThreadPool(40))
        .connectTimeout(timeoutPolicy.connectTimeout)
        .priority(1)
        .build();

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = rateLimiterPolicy.window
    )

    private val parallelRequestsSemaphore = Semaphore(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Processing payment: $paymentId, txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        var paymentUrl = "http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        if (timeoutPolicy.requestTimeout != null) {
            paymentUrl += "&timeout=${timeoutPolicy.requestTimeout}"
        }

        val request = HttpRequest.newBuilder()
            .uri(URI(paymentUrl))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        if (!rateLimiter.tick()) {
            logger.warn("[$accountName] Payment $paymentId delayed due to rate limit")
            rateLimiter.tickBlocking()
        }

        if (!parallelRequestsSemaphore.tryAcquire()) {
            logger.error("[$accountName] [ERROR] Too many parallel requests")
            return
        }

        executePaymentRequest(request, transactionId, paymentId)
            .thenAcceptAsync { response ->
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${response.result}, message: ${response.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(response.result, now(), transactionId, reason = response.message)
                }
            }
            .exceptionally { e ->
                when (e.cause) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
                null
            }
            .whenComplete { _, _ ->
                parallelRequestsSemaphore.release()
            }
    }

    private fun executePaymentRequest(request: HttpRequest, transactionId: UUID, paymentId: UUID): CompletableFuture<ExternalSysResponse> {
        return client
            .sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
            }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()