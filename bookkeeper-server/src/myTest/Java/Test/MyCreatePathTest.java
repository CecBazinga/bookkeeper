package Test;


import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.util.TestZkUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(value= Parameterized.class)
public class MyCreatePathTest {

    private boolean expectedResult;
    private ZooKeeper zkc ;
    private String path;
    private byte[] data;
    private List<ACL> acl;
    private CreateMode createMode;


    // ZooKeeper related variables
    private static ZooKeeperUtil zkUtil = new ZooKeeperUtil();



    @BeforeClass
    public static  void setUp() throws Exception {

        zkUtil.startCluster();

    }

    @AfterClass
    public static void tearDown() throws Exception {

        zkUtil.killCluster();

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() throws IOException {
        return Arrays.asList(new Object[][]{


                {false , "null" , "/ledgers/000/000/000/001" , "data".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE ,
                        CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL},

                {false , "new" , "/ledgers/000/000/000/00b" , "data".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE ,
                        CreateMode.PERSISTENT_WITH_TTL},

                {false , "wrong" , "/ledgers/000/000/000/001" , "data".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE ,
                        CreateMode.CONTAINER},

                {true , "new" , "/ledgers/000/000/000/002" , null , ZooDefs.Ids.OPEN_ACL_UNSAFE ,
                        CreateMode.PERSISTENT_SEQUENTIAL},

                {false , "new" , "alreadyexistingString", new byte[0] , null , CreateMode.EPHEMERAL},

                {false , "new" , "/ledgers/000/000/000/003" , "data".getBytes() ,
                        new ArrayList<ACL>(Collections.singletonList(new ACL())) , CreateMode.EPHEMERAL_SEQUENTIAL},

                {true ,  "new" , "/ledgers/000/000/000/004" , "data".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE ,
                        CreateMode.PERSISTENT}


        });

    }



    public MyCreatePathTest(boolean expectedResult ,String zkc , String path , byte[] data , List<ACL> acl ,
            CreateMode createMode) throws Exception {

        if(zkc == "null"){

            this.zkc = null;

        }else if( zkc == "wrong"){

            this.zkc = new ZooKeeper("wrongString", 10000, null);

        }else if(zkc == "new"){

            this.zkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000, null);
        }


        this.expectedResult = expectedResult;
        this.path = path;
        this.data = data;
        this.acl = acl;
        this.createMode = createMode;

    }

    @Test
    public void createPathTest() {



        boolean realResult;

        if (path == "alreadyexistingString") {

            try {
                /*
                  NB. Sicuramente già la prima chiamata al metodo createFullPathOptimistic genererà un'eccezione
                  poichè data è vuoto e la lista acl è nulla, ma per prepararsi ad un futuro raffinamento dei casi di
                  test si è gia predisposto il caso in cui l'eccezione debba essere sollevata dall'esistenza di uno
                  stesso path , sfruttando le seguenti due righe di codice.
                */

                ZkUtils.createFullPathOptimistic(zkc, "/ledgers/000/000/000/001", data, acl, createMode);
                ZkUtils.createFullPathOptimistic(zkc, "/ledgers/000/000/000/001", data, acl, createMode);
                realResult = true;

            } catch (Exception e) {

                realResult = false;
                e.printStackTrace();
            }

            assertEquals(expectedResult, realResult);

        } else {

            try {

                ZkUtils.createFullPathOptimistic(zkc, path, data, acl, createMode);
                realResult = true;

            } catch (Exception e) {

                realResult = false;
                e.printStackTrace();
            }

            assertEquals(expectedResult, realResult);
        }

    }
}
