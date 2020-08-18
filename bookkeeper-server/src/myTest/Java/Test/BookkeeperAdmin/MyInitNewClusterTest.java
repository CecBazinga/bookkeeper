package Test.BookkeeperAdmin;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookKeeperAdminTest;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(value= Parameterized.class)
public class MyInitNewClusterTest extends BookKeeperClusterTestCase {

    private boolean result;
    private  ServerConfiguration conf;
    private String confType ;

    private static final int numOfBookies = 2;
    private final int lostBookieRecoveryDelayInitValue = 1800;


    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{

                {true ,  "new"},
                {false , "null"},
                {false , "wrong"},

        });

    }



    public MyInitNewClusterTest(boolean result , String conf ){

        super(numOfBookies, 480);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);

        this.result = result;
        this.confType = conf;
    }

    @Test
    public void initNewClusterTest() {

        boolean realResult ;


        if(confType == "null"){

            this.conf = null;

        }else if( confType == "wrong"){

            this.conf = new ServerConfiguration().setMetadataServiceUri("zk+hierarchical://127.0.0.1/ledgers");

        }else if(confType == "new") {

            this.conf = new ServerConfiguration(baseConf);
            String ledgersRootPath = "/testledgers";
            this.conf.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));

        }

        try {

            realResult = BookKeeperAdmin.initNewCluster(conf);

        } catch (Exception e) {
            realResult = false ;
            e.printStackTrace();

        }
        assertEquals(result,realResult);
    }
}
