package org.pcg.walrus.meta.partition;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.common.util.TimeUtil;

/**
 * split days to partition list <br>
 */
public class PartitionSplitor {

	/*
	 * split days into Splits base on Split mode
	 */
	public static List<SPartition> splits(String pMode, Date bDay, Date eDay) {
		int bday = TimeUtil.dateToInt(bDay);
		int eday = TimeUtil.dateToInt(eDay);
		List<SPartition> ps = new ArrayList<SPartition>();
		switch (pMode) {
		// Split all
		case MetaConstants.PARTITION_MODE_A:
			SPartition sa = new SPartition(bDay, eDay);
			sa.setName(MetaConstants.PARTITION_MODE_A);
			ps.add(sa);
			break;
		// Split year
		case MetaConstants.PARTITION_MODE_Y:
			int[] years = TimeUtil.getPartitonYears(bDay, eDay);
			for (int i = 0; i < years.length; i++) {
				int year = years[i];
				int tmpBday = (year * 10000 + 101) > bday ? (year * 10000 + 101) : bday;
				int tmpEday = (year * 10000 + 1231) < eday ? (year * 10000 + 1231) : eday;
				SPartition sy = null;
				try {
					sy = new SPartition(TimeUtil.intToDate(tmpBday), TimeUtil.intToDate(tmpEday));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				sy.setName(String.valueOf(year));
				ps.add(sy);
			}
			break;
		// Split month
		case MetaConstants.PARTITION_MODE_M:
			
			int[] months = TimeUtil.getPartitonMonths(bDay, eDay);
			Date[] bdays = new Date[months.length];
			Date[] edays = new Date[months.length];
			String[] partitions = new String[months.length];
			bdays[0] = bDay;

			Date[] days = TimeUtil.getPartitonDays(bDay, eDay);
			Date tmp = bDay;
			int month = bday / 100;
			partitions[0] = String.valueOf(month);;
			int index = 0;
			for (Date d : days) {
				int tmpMonth = TimeUtil.dateToInt(d) / 100;
				if (tmpMonth != month) {
					edays[index] = tmp;
					bdays[index + 1] = d;
					month = tmpMonth;
					partitions[index + 1] = String.valueOf(month);
					index++;
				}
				tmp = d;
			}
			edays[index] = tmp;
			for(int i=0; i<months.length; i++) {
				SPartition sm = new SPartition(bdays[i], edays[i]);
				sm.setName(partitions[i]);
				ps.add(sm);
			}
			break;
		// Split month
		case MetaConstants.PARTITION_MODE_D:
			Date[] ds = TimeUtil.getPartitonDays(bDay, eDay);
			for(Date d: ds) {
				SPartition dm = new SPartition(d, d);
				dm.setName(TimeUtil.DateToString(d));
				ps.add(dm);
			}
			break;
		// Split hour
		case MetaConstants.PARTITION_MODE_H:
			Date[] ds2 = TimeUtil.getPartitonDays(bDay, eDay);
			for(Date d: ds2) {
				int bhour = 0;
				int ehour = 23;
				boolean first = TimeUtil.dateToInt(bDay) == TimeUtil.dateToInt(d);
				if(first) bhour = TimeUtil.getHour(bDay);
				boolean last = TimeUtil.dateToInt(eDay) == TimeUtil.dateToInt(d);
				if(last) ehour = TimeUtil.getHour(eDay);
				for(int i=bhour; i<=ehour; i++) {
					// hour:00:00 - hour:59:59
					Calendar start = Calendar.getInstance();
					start.setTime(d);
					start.set(Calendar.HOUR_OF_DAY, i);
					start.set(Calendar.MINUTE, 0);
					start.set(Calendar.SECOND, 0);
					Calendar end = Calendar.getInstance();
					end.setTime(d);
					end.set(Calendar.HOUR_OF_DAY, i);
					end.set(Calendar.MINUTE, 59);
					end.set(Calendar.SECOND, 59);
					SPartition dm = new SPartition(start.getTime(), end.getTime());
					String p = TimeUtil.formatDate(start.getTime(), "yyyyMMdd_HH");
					dm.setName(p);
					ps.add(dm);
				}
			}
			break;
		default:
			break;
		}
		return ps;
	}
}
