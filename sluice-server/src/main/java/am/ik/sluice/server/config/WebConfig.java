package am.ik.sluice.server.config;

import java.util.Objects;

import com.samskivert.mustache.Mustache;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.ResourceUrlProvider;

/**
 * Mustache setup: compiler options and the template helpers exposed to every view.
 */
@Configuration(proxyBeanMethods = false)
public class WebConfig implements WebMvcConfigurer {

	private final ObjectProvider<ResourceUrlProvider> resourceUrlProviders;

	@Nullable private ResourceUrlProvider resourceUrlProvider = null;

	public WebConfig(ObjectProvider<ResourceUrlProvider> resourceUrlProviders) {
		this.resourceUrlProviders = resourceUrlProviders;
	}

	/**
	 * Boot's compiler plus {@code emptyStringIsFalse}: an empty value (no version, no
	 * public URL, ...) skips its {@code {{#section}}} instead of rendering it blank.
	 */
	@Bean
	Mustache.Compiler mustacheCompiler(Mustache.TemplateLoader mustacheTemplateLoader) {
		return Mustache.compiler().withLoader(mustacheTemplateLoader).emptyStringIsFalse(true);
	}

	@Override
	public void addInterceptors(InterceptorRegistry registry) {
		if (this.resourceUrlProvider == null) {
			this.resourceUrlProvider = this.resourceUrlProviders.getObject();
		}
		registry.addInterceptor(new HandlerInterceptor() {
			@Override
			public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
					@Nullable ModelAndView modelAndView) {
				if (modelAndView != null) {
					// Resolves a static resource path to its content-hashed URL
					// (spring.web.resources.chain.strategy.content), so assets can be
					// cached for a year and still change with every release.
					modelAndView.addObject("src", (Mustache.Lambda) (frag, out) -> {
						String url = frag.execute();
						String resourceUrl = Objects.requireNonNull(resourceUrlProvider).getForLookupPath(url);
						out.write(StringUtils.hasLength(resourceUrl) ? resourceUrl : url);
					});
				}
			}
		});
	}

}
