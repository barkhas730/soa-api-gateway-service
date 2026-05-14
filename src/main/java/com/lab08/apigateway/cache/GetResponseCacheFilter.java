package com.lab08.apigateway.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Component
public class GetResponseCacheFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(GetResponseCacheFilter.class);

    private final ReactiveStringRedisTemplate redisTemplate;
    private final long cacheTtlSeconds;

    public GetResponseCacheFilter(
            ReactiveStringRedisTemplate redisTemplate,
            @Value("${gateway.cache.ttl-seconds:60}") long cacheTtlSeconds) {
        this.redisTemplate = redisTemplate;
        this.cacheTtlSeconds = cacheTtlSeconds;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        if (exchange.getRequest().getMethod() != HttpMethod.GET) {
            return chain.filter(exchange);
        }

        String key = buildKey(exchange);

        return redisTemplate.opsForValue().get(key)
                .flatMap(cachedBody -> {
                    log.info("CACHE_HIT key={}", key);
                    ServerHttpResponse response = exchange.getResponse();
                    response.setStatusCode(HttpStatus.OK);
                    response.getHeaders().set("X-Cache", "HIT");
                    byte[] bytes = cachedBody.getBytes(StandardCharsets.UTF_8);
                    return response.writeWith(Mono.just(response.bufferFactory().wrap(bytes)));
                })
                .switchIfEmpty(Mono.defer(() -> {
                    log.info("CACHE_MISS key={}", key);
                    exchange.getResponse().getHeaders().set("X-Cache", "MISS");

                    ServerHttpResponse originalResponse = exchange.getResponse();
                    ServerHttpResponseDecorator decoratedResponse = new ServerHttpResponseDecorator(originalResponse) {
                        @Override
                        public Mono<Void> writeWith(org.reactivestreams.Publisher<? extends DataBuffer> body) {
                            return DataBufferUtils.join(Mono.from(body))
                                    .flatMap(dataBuffer -> {
                                        byte[] content = new byte[dataBuffer.readableByteCount()];
                                        dataBuffer.read(content);
                                        DataBufferUtils.release(dataBuffer);

                                        String bodyText = new String(content, StandardCharsets.UTF_8);
                                        HttpStatusCode status = getStatusCode();

                                        if (status != null && status.is2xxSuccessful() && !bodyText.isBlank()) {
                                            return redisTemplate.opsForValue()
                                                    .set(key, bodyText, Duration.ofSeconds(cacheTtlSeconds))
                                                    .doOnSuccess(saved -> log.info("CACHE_SAVE key={} ttl={}s saved={}", key, cacheTtlSeconds, saved))
                                                    .then(Mono.defer(() -> {
                                                        byte[] bytes = bodyText.getBytes(StandardCharsets.UTF_8);
                                                        return super.writeWith(Mono.just(bufferFactory().wrap(bytes)));
                                                    }));
                                        }

                                        return super.writeWith(Mono.just(bufferFactory().wrap(content)));
                                    });
                        }
                    };

                    return chain.filter(exchange.mutate().response(decoratedResponse).build());
                }));
    }

    @Override
    public int getOrder() {
        return -50;
    }

    private String buildKey(ServerWebExchange exchange) {
        String path = exchange.getRequest().getURI().getPath();
        String query = exchange.getRequest().getURI().getRawQuery();
        return "cache:get:" + path + (query == null ? "" : "?" + query);
    }
}
