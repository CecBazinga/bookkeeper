/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the bookkeeper admin.
 */
public class BookKeeperAdminTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperAdminTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final int numOfBookies = 2;
    private final int lostBookieRecoveryDelayInitValue = 1800;

    public BookKeeperAdminTest() {
        super(numOfBookies, 480);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);
    }





    public void testTriggerAudit(boolean storeSystemTimeAsLedgerUnderreplicatedMarkTime) throws Exception {
        ServerConfiguration thisServerConf = new ServerConfiguration(baseConf);
        thisServerConf
                .setStoreSystemTimeAsLedgerUnderreplicatedMarkTime(storeSystemTimeAsLedgerUnderreplicatedMarkTime);
        restartBookies(thisServerConf);
        ClientConfiguration thisClientConf = new ClientConfiguration(baseClientConf);
        thisClientConf
                .setStoreSystemTimeAsLedgerUnderreplicatedMarkTime(storeSystemTimeAsLedgerUnderreplicatedMarkTime);
        long testStartSystime = System.currentTimeMillis();
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(thisClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        int lostBookieRecoveryDelayValue = bkAdmin.getLostBookieRecoveryDelay();
        urLedgerMgr.disableLedgerReplication();
        try {
            bkAdmin.triggerAudit();
            fail("Trigger Audit should have failed because LedgerReplication is disabled");
        } catch (UnavailableException une) {
            // expected
        }
        assertEquals("LostBookieRecoveryDelay", lostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        urLedgerMgr.enableLedgerReplication();
        bkAdmin.triggerAudit();
        assertEquals("LostBookieRecoveryDelay", lostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        long ledgerId = 1L;
        LedgerHandle ledgerHandle = bkc.createLedgerAdv(ledgerId, numBookies, numBookies, numBookies, digestType,
                PASSWORD.getBytes(), null);
        ledgerHandle.addEntry(0, "data".getBytes());
        ledgerHandle.close();

        BookieServer bookieToKill = bs.get(1);
        killBookie(1);
        /*
         * since lostBookieRecoveryDelay is set, when a bookie is died, it will
         * not start Audit process immediately. But when triggerAudit is called
         * it will force audit process.
         */
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<UnderreplicatedLedger> underreplicatedLedgerItr = urLedgerMgr.listLedgersToRereplicate(null);
        assertTrue("There are supposed to be underreplicatedledgers", underreplicatedLedgerItr.hasNext());
        UnderreplicatedLedger underreplicatedLedger = underreplicatedLedgerItr.next();
        assertEquals("Underreplicated ledgerId", ledgerId, underreplicatedLedger.getLedgerId());
        assertTrue("Missingreplica of Underreplicated ledgerId should contain " + bookieToKill.getLocalAddress(),
                underreplicatedLedger.getReplicaList().contains(bookieToKill.getLocalAddress().toString()));
        if (storeSystemTimeAsLedgerUnderreplicatedMarkTime) {
            long ctimeOfURL = underreplicatedLedger.getCtime();
            assertTrue("ctime of underreplicated ledger should be greater than test starttime",
                    (ctimeOfURL > testStartSystime) && (ctimeOfURL < System.currentTimeMillis()));
        } else {
            assertEquals("ctime of underreplicated ledger should not be set", UnderreplicatedLedger.UNASSIGNED_CTIME,
                    underreplicatedLedger.getCtime());
        }
        bkAdmin.close();
    }


    void initiateNewClusterAndCreateLedgers(ServerConfiguration newConfig, List<String> bookiesRegPaths)
            throws Exception {
        Assert.assertTrue("New cluster should be initialized successfully", BookKeeperAdmin.initNewCluster(newConfig));

        /**
         * create znodes simulating existence of Bookies in the cluster
         */
        int numberOfBookies = 3;
        Random rand = new Random();
        for (int i = 0; i < numberOfBookies; i++) {
            String ipString = InetAddresses.fromInteger(rand.nextInt()).getHostAddress();
            bookiesRegPaths.add(ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
                + "/" + AVAILABLE_NODE + "/" + ipString + ":3181");
            zkc.create(bookiesRegPaths.get(i), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        /*
         * now it should be possible to create ledger and delete the same
         */
        BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
        LedgerHandle lh;
        int numOfLedgers = 5;
        for (int i = 0; i < numOfLedgers; i++) {
            lh = bk.createLedger(numberOfBookies, numberOfBookies, numberOfBookies, BookKeeper.DigestType.MAC,
                    new byte[0]);
            lh.close();
        }
        bk.close();
    }



    public void testGetListOfEntriesOfLedger(boolean isLedgerClosed) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        int numOfEntries = 6;
        BookKeeper bkc = new BookKeeper(conf);
        LedgerHandle lh = bkc.createLedger(numOfBookies, numOfBookies, digestType, "testPasswd".getBytes());
        long lId = lh.getId();
        for (int i = 0; i < numOfEntries; i++) {
            lh.addEntry("000".getBytes());
        }
        if (isLedgerClosed) {
            lh.close();
        }
        try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
            for (int i = 0; i < bs.size(); i++) {
                CompletableFuture<AvailabilityOfEntriesOfLedger> futureResult = bkAdmin
                        .asyncGetListOfEntriesOfLedger(bs.get(i).getLocalAddress(), lId);
                AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = futureResult.get();
                assertEquals("Number of entries", numOfEntries,
                        availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
                for (int j = 0; j < numOfEntries; j++) {
                    assertTrue("Entry should be available: " + j, availabilityOfEntriesOfLedger.isEntryAvailable(j));
                }
                assertFalse("Entry should not be available: " + numOfEntries,
                        availabilityOfEntriesOfLedger.isEntryAvailable(numOfEntries));
            }
        }
        bkc.close();
    }



    private void testBookieServiceInfo(boolean readonly, boolean legacy) throws Exception {
        File tmpDir = createTempDir("bookie", "test");
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[]{tmpDir.getPath()})
                .setBookiePort(PortManager.nextFreePort())
                .setMetadataServiceUri(metadataServiceUri);

        LifecycleComponent server = Main.buildBookieServer(new BookieConfiguration(conf));
        // 2. start the server
        CompletableFuture<Void> stackComponentFuture = ComponentStarter.startComponent(server);
        while (server.lifecycleState() != Lifecycle.State.STARTED) {
            Thread.sleep(100);
        }

        ServerConfiguration bkConf = newServerConfiguration().setForceReadOnlyBookie(readonly);
        BookieServer bkServer = startBookie(bkConf);

        String bookieId = bkServer.getLocalAddress().toString();
        String host = bkServer.getLocalAddress().getHostName();
        int port = bkServer.getLocalAddress().getPort();

        if (legacy) {
            String regPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(bkConf) + "/" + AVAILABLE_NODE;
            regPath = readonly
                    ? regPath + READONLY + "/" + bookieId
                    : regPath + "/" + bookieId;
            // deleting the metadata, so that the bookie registration should
            // continue successfully with legacy BookieServiceInfo
            zkc.setData(regPath, new byte[]{}, -1);
        }

        try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
            BookieServiceInfo bookieServiceInfo = bkAdmin.getBookieServiceInfo(bookieId);

            assertThat(bookieServiceInfo.getEndpoints().size(), is(1));
            BookieServiceInfo.Endpoint endpoint = bookieServiceInfo.getEndpoints().stream()
                    .filter(e -> Objects.equals(e.getId(), bookieId))
                    .findFirst()
                    .get();
            assertNotNull("Endpoint " + bookieId + " not found.", endpoint);

            assertThat(endpoint.getHost(), is(host));
            assertThat(endpoint.getPort(), is(port));
            assertThat(endpoint.getProtocol(), is("bookie-rpc"));
        }

        bkServer.shutdown();
        stackComponentFuture.cancel(true);
    }

}
