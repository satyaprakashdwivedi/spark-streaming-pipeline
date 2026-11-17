package io.github.satyaprakashdwivedi.streaming.components.dbt;

import java.util.Map;

/** Exposes the package-private SQL rewrite to tests in other packages. */
public final class DbtModelTransformTestAccess {
    private DbtModelTransformTestAccess() {}

    public static String rewrite(String sql, String streamSchema, Map<String, String> relations) {
        return DbtModelTransform.rewriteRelations(sql, streamSchema, relations);
    }
}
