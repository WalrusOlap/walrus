package org.pcg.walrus.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

/**
 * <code>TimeUtil</code> </br>
 * time function
 * 
 * @author dannyliu
 * @version 2017/7/19
 */
public class TimeUtil {

	// SimpleDateFormat is not thread safe
//	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
//	private static SimpleDateFormat monthFormatter = new SimpleDateFormat("yyyyMM");

	private static int DEFAULT_BDAY = 20180101;
	private static int DEFAULT_EDAY = 20301231;
	private static String META_VALID_DAY_ALL = "all";

	/**
	 * check if int date valid
	 */
	public static boolean isDateValid(int date) {
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			formatter.parse(String.valueOf(date));
		} catch (ParseException e) {
			return false;
		}
		return true;
	}

	/**
	 * @param bday
	 * @param eday
	 * @return days_diff(bday, eday)
	 */
	public static int[] splitDays(Date bday, Date eday, int range) {
		int[] daySplit = new int[4];
		int b_day = dateToInt(bday);
		int e_day = dateToInt(eday);
		if (bday.after(eday)) {
			daySplit[0] = b_day;
			daySplit[1] = b_day;
			daySplit[2] = b_day;
			daySplit[3] = b_day;
		} else {
			Date[] days = getPartitonDays(bday, eday);
			daySplit[0] = b_day;
			daySplit[1] = dateToInt(days[days.length - range - 2]);
			daySplit[2] = dateToInt(days[days.length - range - 1]);
			daySplit[3] = e_day;
		}
		return daySplit;
	}

	/**
	 * @param bday
	 * @param eday
	 * @return days_diff(bday, eday)
	 */
	public static int diffDays(String bday, String eday) {
		int diff = 0;
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			Date bDay = formatter.parse(bday);
			Date eDay = formatter.parse(eday);
			return diffDays(bDay, eDay);
		} catch (ParseException e) {
			// do nothing
		}
		return diff;
	}

	/**
	 * @param bday
	 * @param eday
	 * @return days_diff(bday, eday)
	 */
	public static int diffDays(Date bday, Date eday) {
		return (int) TimeUnit.DAYS.convert(eday.getTime() - bday.getTime(), TimeUnit.MILLISECONDS);
	}

	/**
	 * @param bday
	 * @param eday
	 * @return hours_diff(bday, eday)
	 */
	public static int diffHours(Date bday, Date eday) {
		return (int) ((eday.getTime() - bday.getTime()) / (60 * 60 * 1000));
	}


	/**
	 * @return today in 'yyyyMMdd'
	 */
	public static String getToday() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		return formatter.format(new Date());
	}
	
	/**
	 * @return today in 'yyyyMMdd'
	 */
	public static String now() {
		SimpleDateFormat formatterNow = new SimpleDateFormat("yyyyMMddHHmmss");
		return formatterNow.format(new Date());
	}

	/**
	 * @return current month in 'yyyyMM'
	 */
	public static String getCurrentMonth() {
		SimpleDateFormat monthFormatter = new SimpleDateFormat("yyyyMM");
		return monthFormatter.format(new Date());
	}

	/**
	 * split years
	 */
	public static int[] getPartitonYears(Date bday, Date eday) {
		int b_day = dateToInt(bday);
		int e_day = dateToInt(eday);
		int begin = b_day / 10000;
		int end = e_day / 10000;
		int range = end - begin;
		int[] years = new int[range + 1];
		for (int i = 0; i <= range; i++) {
			years[i] = begin + i;
		}
		return years;
	}

	/**
	 * split months
	 */
	public static int[] getPartitonMonths(Date bday, Date eday) {
		int b_day = dateToInt(bday);
		int e_day = dateToInt(eday);
		if (bday == eday)
			return new int[] { b_day / 100 };
		Calendar begin = Calendar.getInstance();
		begin.set(Calendar.YEAR, b_day / 10000);
		begin.set(Calendar.MONTH, b_day / 100 % 100 - 1);
		begin.set(Calendar.DATE, b_day % 100);

		Calendar end = Calendar.getInstance();
		end.set(Calendar.YEAR, e_day / 10000);
		end.set(Calendar.MONTH, e_day / 100 % 100 - 1);
		end.set(Calendar.DATE, e_day % 100);

		int range = 12 * (end.get(Calendar.YEAR) - begin.get(Calendar.YEAR)) + 1 + end.get(Calendar.MONTH) - begin.get(Calendar.MONTH);
		int[] months = new int[range];
		int index = 0;
		while (index < range) {
			months[index++] = begin.get(Calendar.YEAR) * 100 + begin.get(Calendar.MONTH) + 1;
			begin.add(Calendar.MONTH, 1);
		}
		return months;
	}

	/**
	 * split days
	 * begin.set(Calendar.HOUR_OF_DAY, 12);
	 * begin.set(Calendar.MINUTE, 0);
	 * begin.set(Calendar.SECOND, 0);
	 * begin.set(Calendar.MILLISECOND, 0);
	 */
	public static Date[] getPartitonDays(Date b_day, Date e_day) {
		int bday = dateToInt(b_day);
		int eday = dateToInt(e_day);
		Calendar begin = Calendar.getInstance();
		begin.set(Calendar.YEAR, bday / 10000);
		begin.set(Calendar.MONTH, bday / 100 % 100 - 1);
		begin.set(Calendar.DATE, bday % 100);
		begin.set(Calendar.HOUR_OF_DAY, 12);
		begin.set(Calendar.MINUTE, 0);
		begin.set(Calendar.SECOND, 0);
		begin.set(Calendar.MILLISECOND, 0);

		Calendar end = Calendar.getInstance();
		end.set(Calendar.YEAR, eday / 10000);
		end.set(Calendar.MONTH, eday / 100 % 100 - 1);
		end.set(Calendar.DATE, eday % 100 + 1);
		end.set(Calendar.HOUR_OF_DAY, 12);
		end.set(Calendar.MINUTE, 0);
		end.set(Calendar.SECOND, 0);
		end.set(Calendar.MILLISECOND, 0);

		int range = (int) ((end.getTimeInMillis() - begin.getTimeInMillis()) / (24 * 60 * 60 * 1000));
		range = range < 1 ? 1 : range;
		Date[] days = new Date[range];
		for (int index = 0; index < range; index++) {
			days[index] = begin.getTime();
			begin.add(Calendar.DATE, 1);
		}
		return days;
	}

	// check if days between bday and eday are all valid:
	// 111111110000000000000000000000
	public static Set<Integer> unsupportedDays(String validDays, Date b_day, Date e_day) {
		Set<Integer> set = new HashSet<Integer>();
		if (META_VALID_DAY_ALL.equalsIgnoreCase(validDays) || StringUtils.isBlank(validDays))
			return set;
		int bday = dateToInt(b_day);
		int eday = dateToInt(e_day);
		int bIndex = bday / 100 * 100 + 1;
		for (int i = bday; i <= eday; i++) {
			if ('1' != validDays.charAt(i - bIndex))
				set.add(i);
		}
		return set;
	}

	/**
	 * @return yesterday yyyymmdd
	 */
	public static String getYesterday() {
		return getNdaysAgo(1);
	}

	/**
	 * @return N days befor today
	 */
	public static String getNdaysAgo(int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DATE, -n);
		return new SimpleDateFormat("yyyyMMdd").format(calendar.getTime());
	}

	/**
	 * @return N days after today
	 */
	public static String getNdaysAfter(int n) {
		return getNdaysAgo(-n);
	}

	/**
	 * date to int
	 */
	public static int dateToInt(Date date) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		return Integer.parseInt(formatter.format(date));
	}


	/**
	 * date year
	 */
	public static int getYear(Date date) {
		return Integer.parseInt(new SimpleDateFormat("yyyy").format(date));
	}


	/**
	 * date month
	 */
	public static int getMonth(Date date) {
		return Integer.parseInt(new SimpleDateFormat("yyyyMM").format(date));
	}


	/**
	 * date hour
	 */
	public static int getHour(Date date) {
		return Integer.parseInt(new SimpleDateFormat("HH").format(date));
	}

	/**
	 * date minute
	 */
	public static int getMinute(Date date) {
		return Integer.parseInt(new SimpleDateFormat("mm").format(date));
	}

	/**
	 * date to int
	 * @throws ParseException 
	 */
	public static Date intToDate(int date) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		return formatter.parse(date+"");
	}


	/**
	 * string to date
	 * @throws ParseException
	 */
	public static Date stringToDate(String date, String format) throws ParseException {
		return new SimpleDateFormat(format).parse(date);
	}

	/**
	 * string to date
	 * @throws ParseException 
	 */
	public static Date stringToDate(String date) throws ParseException {
		String[] pattens = new String[] {
			"yyyyMMdd",
			"yyyyMMdd HH:mm:ss",
			"yyyy-MM-dd",
			"yyyy-MM-dd HH:mm:ss"
		};
		return DateUtils.parseDate(date, pattens);
	}
	
	/**
	 * return date string in {yyyyMMdd} format
	 */
	public static String DateToString(Date date) {
		return DateToString(date, "yyyyMMdd");
	}

	/**
	 * return date string in {yyyyMMdd} format
	 */
	public static String DateToString(Date date, String format) {
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		return formatter.format(date);
	}
	
	/**
	 * return date string in {format}
	 */
	public static String formatDate(Date date, String format) {
		return new SimpleDateFormat(format).format(date);
	}

	/**
	 * Constants.DEFAULT_BDAY
	 * @throws ParseException 
	 */
	public static Date defaultBday() {
		try {
			return intToDate(DEFAULT_BDAY);
		} catch (ParseException e) {
			return null;
		}
	}
	
	/**
	 * Constants.DEFAULT_EDAY
	 * @throws ParseException 
	 */
	public static Date defaultEday() {
		try {
			return intToDate(DEFAULT_EDAY);
		} catch (ParseException e) {
			return null;
		}	}
	
	// 1483958256(Mon Jan  9 18:37:36 CST 2017) > 201701
	public static int parseMonth(long timestamp) {
		return format(timestamp, "yyyyMM", 199001);
	}
	
	// 1483958256(Mon Jan  9 18:37:36 CST 2017) > 20170109
	public static int parseDay(long timestamp) {
		return format(timestamp, "yyyyMMdd", 19900101);
	}
  
	// 1483958256(Mon Jan  9 18:37:36 CST 2017) > 18
	public static int parseHour(long timestamp) {
		return format(timestamp, "HH", -1);
	}

	// 1483958256(Mon Jan  9 18:37:36 CST 2017) > 37
	public static int parseMinute(long timestamp) {
		return format(timestamp, "m", -1);
	}
	
	// 1483958256(Mon Jan  9 18:37:36 CST 2017) > 2
	public static int parseWeek(long timestamp) {
		return format(timestamp, "w", -1);
	}
	
	// 1483958256(Mon Jan  9 18:37:36 CST 2017) > 0
	public static int parseWday(long timestamp) {
		return format(timestamp, "u", -1);
	}

	// format date
	private static int format(long timestamp, String format, int defaultVal) {
		try {
			return Integer.parseInt(new SimpleDateFormat(format).format(timestamp * 1000));
		} catch (Exception e) {
			return defaultVal;
		}
	}
}
