package org.apache.bookkeeper.util;

import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;

import javax.sound.midi.SysexMessage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value= Parameterized.class)

public class ZkUtilsGetChildrenTest  {


    private boolean expectedResult;
    private ZooKeeper zkc ;
    private String path;
    private long timeout;
    private static List<String> paths = Arrays.asList("/ledgers/000/000/000/001", "/ledgers/000/000/000/002",
            "/ledgers/000/000/000/003");
    private static List<String> childPaths = Arrays.asList("001", "002", "003");
    // ZooKeeper related variables
    private static ZooKeeperUtil zkUtil = new ZooKeeperUtil();

    /*
    @Mock
    ZkUtils.GetChildrenCtx mocked = mock(ZkUtils.GetChildrenCtx.class) ;
*/

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


                {false , "null" , "/ledgers/000/000/000/004" , 0},

                {true , "new" , "/ledgers/000/000/000" , 100 },

                {false , "wrong" , "/ledgers/000/000/000/00b" , -1},

                {false , "new" , "/ledgers/000/000/00b" , 0 },//aggiunto per migliorare statement e branch coverage

                {false , "new" , "/ledgers/000/000/003" , 1 },//aggiunto per migliorare statement e branch coverage

                //{false , "mock" , "/ledgers/000/000" , 1000 },//aggiunto per migliorare statement coverage

        });

    }


    public ZkUtilsGetChildrenTest(boolean expectedResult ,String zkc , String path , long timeout) throws IOException {

        if(zkc == "null"){

            this.zkc = null;

        }else if( zkc == "wrong"){

            this.zkc = new ZooKeeper("wrongString", 10000, null);

        }else if(zkc == "new"){

            this.zkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000, null);

        }else if(zkc == "mock"){


            this.zkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000, null);
            //when(mocked).thenThrow(new InterruptedException());
        }

        this.expectedResult = expectedResult;
        this.path = path;
        this.timeout = timeout;


    }

    @Test
    public void testGetChildrenInSingleNode() {

        boolean realResult = true ;

        try {


            List<String> children = ZkUtils.getChildrenInSingleNode(zkc, path, timeout);

            assertThat(children, is(childPaths));



        } catch (Exception e) {

            realResult = false;
            e.printStackTrace();

            assertEquals(expectedResult, realResult);
        }



    }

 }
