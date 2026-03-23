package io.dist.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dist.model.Message;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Evaluates single-condition filter expressions against a {@link Message}.
 *
 * <h3>Expression format</h3>
 * <pre>{@code <path> <op> <value>}</pre>
 *
 * <ul>
 *   <li><b>path</b> – {@code routingKey}, {@code exchange}, {@code queueName},
 *       or a dot-separated JSON path rooted at {@code payload} (e.g.
 *       {@code payload.type}, {@code payload.order.amount})</li>
 *   <li><b>op</b> – {@code ==}, {@code !=}, {@code >}, {@code >=},
 *       {@code <}, {@code <=}, {@code contains}</li>
 *   <li><b>value</b> – double-quoted string, number, or {@code true}/{@code false}</li>
 * </ul>
 *
 * <p>A {@code null} or blank expression is treated as pass-all (returns
 * {@code true}). If the expression cannot be parsed the evaluator logs a
 * warning and also returns {@code true} so processing is not silently
 * dropped.</p>
 */
@ApplicationScoped
public class FilterEvaluator {

    private static final Logger LOG = Logger.getLogger(FilterEvaluator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Matches: <path> <op> <value> with optional surrounding whitespace. */
    private static final Pattern EXPR_PATTERN = Pattern.compile(
            "^(\\S+)\\s*(==|!=|>=|<=|>|<|contains)\\s*(.+)$"
    );

    /**
     * Evaluates the given expression against the message.
     *
     * @param expression the filter expression (may be {@code null} for pass-all)
     * @param message    the message to test
     * @return {@code true} if the message passes the filter
     */
    public boolean evaluate(String expression, Message message) {
        if (expression == null || expression.isBlank()) return true;
        Matcher m = EXPR_PATTERN.matcher(expression.trim());
        if (!m.matches()) {
            LOG.warnf("Invalid filter expression: %s", expression);
            return true; // pass-all on parse failure
        }
        String path = m.group(1);
        String op = m.group(2);
        String rawValue = m.group(3).trim();
        Object actual = resolvePath(path, message);
        return compare(actual, op, rawValue);
    }

    /**
     * Resolves a path against the message, returning the raw field value.
     * Returns {@code null} if the path cannot be resolved.
     */
    private Object resolvePath(String path, Message message) {
        if ("routingKey".equals(path)) return message.routingKey;
        if ("exchange".equals(path)) return message.exchange;
        if ("queueName".equals(path)) return message.queueName;
        if (path.startsWith("payload.")) {
            String jsonPath = path.substring("payload.".length());
            try {
                if (message.payload == null) return null;
                JsonNode root = MAPPER.readTree(message.payload);
                String[] parts = jsonPath.split("\\.");
                JsonNode node = root;
                for (String part : parts) {
                    if (node == null || node.isMissingNode()) return null;
                    node = node.get(part);
                }
                if (node == null || node.isNull()) return null;
                if (node.isNumber()) return node.doubleValue();
                if (node.isBoolean()) return node.booleanValue();
                return node.asText();
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Resolves a dot-path within a JSON string, returning the raw value as a
     * String (for use as a stateKey in aggregation groupBy).
     *
     * @param jsonPath the dot-separated JSON path (e.g. {@code type}, {@code order.region})
     * @param json     the JSON string to navigate
     * @return the string value at the path, or {@code null}
     */
    public String resolveJsonPath(String jsonPath, String json) {
        if (json == null || jsonPath == null) return null;
        try {
            JsonNode root = MAPPER.readTree(json);
            String[] parts = jsonPath.split("\\.");
            JsonNode node = root;
            for (String part : parts) {
                if (node == null || node.isMissingNode()) return null;
                node = node.get(part);
            }
            if (node == null || node.isNull()) return null;
            return node.asText();
        } catch (Exception e) {
            return null;
        }
    }

    private boolean compare(Object actual, String op, String rawValue) {
        if (actual == null) return false;

        // Determine expected value type from the raw literal
        Object expected;
        if (rawValue.startsWith("\"") && rawValue.endsWith("\"")) {
            expected = rawValue.substring(1, rawValue.length() - 1);
        } else if ("true".equals(rawValue)) {
            expected = Boolean.TRUE;
        } else if ("false".equals(rawValue)) {
            expected = Boolean.FALSE;
        } else {
            try {
                expected = Double.parseDouble(rawValue);
            } catch (NumberFormatException e) {
                expected = rawValue;
            }
        }

        switch (op) {
            case "==": return actualEqualsExpected(actual, expected);
            case "!=": return !actualEqualsExpected(actual, expected);
            case "contains": return actual.toString().contains(expected.toString());
            case ">": case ">=": case "<": case "<=":
                return numericCompare(actual, op, expected);
            default:
                return false;
        }
    }

    private boolean actualEqualsExpected(Object actual, Object expected) {
        if (actual instanceof Double && expected instanceof Double) {
            return actual.equals(expected);
        }
        return actual.toString().equals(expected.toString());
    }

    private boolean numericCompare(Object actual, String op, Object expected) {
        try {
            double a = Double.parseDouble(actual.toString());
            double e = Double.parseDouble(expected.toString());
            switch (op) {
                case ">":  return a > e;
                case ">=": return a >= e;
                case "<":  return a < e;
                case "<=": return a <= e;
            }
        } catch (NumberFormatException ex) {
            // non-numeric comparison — fall through to false
        }
        return false;
    }
}
