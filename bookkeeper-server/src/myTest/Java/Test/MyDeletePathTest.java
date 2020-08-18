package Test;

import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(value= Parameterized.class)
public class MyDeletePathTest {

    private boolean expectedResult;
    private ZooKeeper zkc ;
    private String path;
    private int znodeVersion;
    private static List<String> paths = Arrays.asList("/ledgers/000/000/000/001", "/ledgers/000/000/000/002",
                                          "/ledgers/000/000/000/003");
    // ZooKeeper related variables
    private static ZooKeeperUtil zkUtil = new ZooKeeperUtil();



    @BeforeClass
    public static  void setUp() throws Exception {

        zkUtil.startCluster();
        ZooKeeper initializerZkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000, null);

        for (String path : paths ){

            ZkUtils.createFullPathOptimistic(initializerZkc, path, "data".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE ,
                    CreateMode.CONTAINER);

        }
    }

    @AfterClass
    public static void tearDown() throws Exception {

        zkUtil.killCluster();

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() throws IOException {
        return Arrays.asList(new Object[][]{


                {false , "null" , "/ledgers/000/000/000/004" , 1},

                {true , "new" , "/ledgers/000/000/000/001" , -1 },

                {false , "wrong" , "/ledgers/000/000/000/00b" , 0},

        });

    }



    public MyDeletePathTest(boolean expectedResult ,String zkc , String path , int znodeVersion) throws IOException {

        if(zkc == "null"){

            this.zkc = null;

        }else if( zkc == "wrong"){

            this.zkc = new ZooKeeper("wrongString", 10000, null);

        }else if(zkc == "new"){

            this.zkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000, null);
        }

        this.expectedResult = expectedResult;
        this.path = path;
        this.znodeVersion = znodeVersion;


    }

    @Test
    public void deletePathTest() {



        boolean realResult;

            try {

                ZkUtils.deleteFullPathOptimistic(zkc, path, znodeVersion);
                realResult = true;

            } catch (Exception e) {

                realResult = false;
                e.printStackTrace();
            }

            assertEquals(expectedResult, realResult);
        }


}
