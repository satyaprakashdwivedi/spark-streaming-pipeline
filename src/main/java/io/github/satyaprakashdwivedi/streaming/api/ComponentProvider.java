package io.github.satyaprakashdwivedi.streaming.api;

import io.github.satyaprakashdwivedi.streaming.registry.ComponentRegistry;

/**
 * Plug-in point for your own components. Implement it, list the class in
 * {@code META-INF/services/io.github.satyaprakashdwivedi.streaming.api.ComponentProvider}, put the jar on the
 * classpath, and the types you register become usable in pipeline configs.
 */
public interface ComponentProvider {
    void register(ComponentRegistry registry);
}
