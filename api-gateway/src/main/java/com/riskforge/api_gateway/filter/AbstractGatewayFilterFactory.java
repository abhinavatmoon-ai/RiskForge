package com.riskforge.api_gateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilter;

public abstract class AbstractGatewayFilterFactory<T> extends org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory<T> {
    public AbstractGatewayFilterFactory(Class<T> configClass) {
        super(configClass);
    }

    @Override
    public abstract GatewayFilter apply(T config);
}
