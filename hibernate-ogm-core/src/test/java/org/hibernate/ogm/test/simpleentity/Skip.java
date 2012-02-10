package org.hibernate.ogm.test.simpleentity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;
import org.hibernate.annotations.common.util.StringHelper;
import org.hibernate.dialect.Dialect;
import org.hibernate.testing.SkipForDialect;

public class Skip {

	private static final Logger log = Logger.getLogger(Skip.class);
	private final String reason;
	private final String testDescription;
	public static final Skip SKIP = new Skip("", "");

	private Skip(String reason, String testDescription) {
		this.reason = reason;
		this.testDescription = testDescription;
	}

	public final Skip determineSkipByDialect(Dialect dialect, Method runMethod,
			String fullTestName) throws Exception {
		// skips have precedence, so check them first
		SkipForDialect skipForDialectAnn = locateAnnotation(
				SkipForDialect.class, runMethod);
		if (skipForDialectAnn != null) {
			for (Class<? extends Dialect> dialectClass : skipForDialectAnn
					.value()) {
				if (skipForDialectAnn.strictMatching()) {
					if (dialectClass.equals(dialect.getClass())) {
						return buildSkip(dialect, skipForDialectAnn.comment(),
								skipForDialectAnn.jiraKey(), fullTestName);
					}
				} else {
					if (dialectClass.isInstance(dialect)) {
						return buildSkip(dialect, skipForDialectAnn.comment(),
								skipForDialectAnn.jiraKey(), fullTestName);
					}
				}
			}
		}
		return null;
	}

	private <T extends Annotation> T locateAnnotation(Class<T> annotationClass,
			Method runMethod) {
		T annotation = runMethod.getAnnotation(annotationClass);
		if (annotation == null) {
			annotation = getClass().getAnnotation(annotationClass);
		}
		if (annotation == null) {
			annotation = runMethod.getDeclaringClass().getAnnotation(
					annotationClass);
		}
		return annotation;
	}

	public Skip buildSkip(Dialect dialect, String comment, String jiraKey,
			String fullTestName) {
		StringBuilder buffer = new StringBuilder();
		buffer.append("skipping database-specific test [");
		buffer.append(fullTestName);
		buffer.append("] for dialect [");
		buffer.append(dialect.getClass().getName());
		buffer.append(']');

		if (StringHelper.isNotEmpty(comment)) {
			buffer.append("; ").append(comment);
		}

		if (StringHelper.isNotEmpty(jiraKey)) {
			buffer.append(" (").append(jiraKey).append(')');
		}

		return new Skip(buffer.toString(), null);
	}

	public void reportSkip(String fullTestName) {
		reportSkip(this.reason,this.testDescription, fullTestName);
	}

	private void reportSkip(String reason, String testDescription,
			String fullTestName) {
		StringBuilder builder = new StringBuilder();
		builder.append("*** skipping test [");
		builder.append(fullTestName);
		builder.append("] - ");
		builder.append(testDescription);
		builder.append(" : ");
		builder.append(reason);
		log.warn(builder.toString());
	}
}
