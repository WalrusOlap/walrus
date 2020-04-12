package org.pcg.walrus.meta.test;

import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.junit.Test;

import org.pcg.walrus.meta.MetaContainer;
import org.pcg.walrus.meta.partition.PartitionSplitor;
import org.pcg.walrus.meta.tree.MTree;
import org.pcg.walrus.common.util.TimeUtil;

public class MetaTest {

	@Test
	public void testMContainer() throws InterruptedException {
		final MetaContainer container = new MetaContainer();
		for (int i = 0; i < 10; i++) {
			new Thread() {
				public void run() {
					MTree tree = new MTree();
					container.update(tree);
				};
			}.start();
		}
		Thread.sleep(1000);
		assertTrue(container.currentTree() != null);
		container.switchTree();
		assertTrue(container.currentTree() != null);
	}

	@Test
	public void testPartition() throws InterruptedException, ParseException {
		assertTrue(PartitionSplitor.splits("Y", TimeUtil.intToDate(20170828), TimeUtil.intToDate(20170902)).size() == 1);
		assertTrue(PartitionSplitor.splits("M", TimeUtil.intToDate(20170828), TimeUtil.intToDate(20170902)).size() == 2);
		assertTrue(PartitionSplitor.splits("D", TimeUtil.intToDate(20170828), TimeUtil.intToDate(20170902)).size() == 6);
		assertTrue(PartitionSplitor.splits("H", TimeUtil.stringToDate("20170828 08:00:00"), TimeUtil.stringToDate("20170828 08:59:59")).get(0).getName().equalsIgnoreCase("20170828_08"));
		assertTrue(PartitionSplitor.splits("H", TimeUtil.stringToDate("20170828 12:00:00"), TimeUtil.stringToDate("20170828 13:59:59")).size() == 2);
		assertTrue(PartitionSplitor.splits("H", TimeUtil.stringToDate("20170828 12:00:00"), TimeUtil.stringToDate("20170902 23:59:59")).size() == 132);
	}
}
