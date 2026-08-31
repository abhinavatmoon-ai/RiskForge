package com.riskforge.api_gateway.config;

import java.net.InetSocketAddress;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class GatewayConfiguration {

    @Bean
    KeyResolver ipKeyResolver() {
        return exchange -> {
            InetSocketAddress remoteAddress = exchange.getRequest().getRemoteAddress();
            String address = remoteAddress == null || remoteAddress.getAddress() == null
                    ? "unknown"
                    : remoteAddress.getAddress().getHostAddress();
            return Mono.just(address);
        };
    }
}
