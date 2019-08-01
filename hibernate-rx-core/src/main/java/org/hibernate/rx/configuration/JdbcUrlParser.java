package org.hibernate.rx.configuration;

import java.net.URI;

/**
 * Utility class to convert url
 */
public class JdbcUrlParser {

	public static URI parse(String url) {
		if ( url == null ) {
			return null;
		}

		if ( url.startsWith( "jdbc:" ) ) {
			return URI.create( url.substring( 5 ) );
		}

		return URI.create( url );
	}
}
