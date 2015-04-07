package com.sisifo.rest;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;

import junit.framework.Assert;

import org.junit.Test;

public class SearchTweetsTest {
	
	@Test
	public void searchTweets() {		
		Calendar cal = new GregorianCalendar();
		cal.set(2015, Calendar.MARCH, 16, 0, 0, 0);
		Date minDate = cal.getTime();
		//cal.set(2015, Calendar.MARCH, 26, 0, 0, 0);
		//Date maxDate = cal.getTime();
		/*
		Collection<String> queries = Arrays.asList("@psebastianbueno","@ahorapodemos","@pablo_iglesias_","@ciudadanoscs",
				"@albert_rivera","@ppopular","@marianorajoy","@psoe","@sanchezcastejon","@upyd","@rosadiezupyd",
				"@_anapastor_","@iescolar","@pedroj_ramirez","@abarceloh25","@ccarnicero","@melchormiralles",
				"@garcia_abadillo","@pacomarhuenda","@fgarea","@montsehuffpost","@carloscuestaem","@alfonsomerlos",
				"@oneto_p","@antonio_cano","@bieitorubido");
				*/
		Collection<String> queries = Arrays.asList("@pedroj_ramirez");
		try {
			SearchTweetsRestApi.run(queries, minDate, null, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		
	}

}
