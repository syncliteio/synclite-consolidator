/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.consolidator.web;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

@WebFilter("/*")
public class CsrfFilter implements Filter {

	private static final String CSRF_TOKEN_ATTR = "csrfToken";
	private static final String CSRF_TOKEN_PARAM = "csrfToken";
	private static final SecureRandom secureRandom = new SecureRandom();

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
	}

	@Override
	public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest request = (HttpServletRequest) req;
		HttpServletResponse response = (HttpServletResponse) res;

		if ("POST".equalsIgnoreCase(request.getMethod())) {
			HttpSession session = request.getSession(false);
			if (session == null) {
				response.sendError(HttpServletResponse.SC_FORBIDDEN, "Session expired");
				return;
			}
			String sessionToken = (String) session.getAttribute(CSRF_TOKEN_ATTR);
			String requestToken = request.getParameter(CSRF_TOKEN_PARAM);
			if (sessionToken == null || !sessionToken.equals(requestToken)) {
				response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid CSRF token");
				return;
			}
		}

		// Ensure a CSRF token exists in the session
		HttpSession session = request.getSession(true);
		if (session.getAttribute(CSRF_TOKEN_ATTR) == null) {
			byte[] bytes = new byte[32];
			secureRandom.nextBytes(bytes);
			session.setAttribute(CSRF_TOKEN_ATTR, Base64.getUrlEncoder().withoutPadding().encodeToString(bytes));
		}

		chain.doFilter(request, response);
	}

	@Override
	public void destroy() {
	}
}
