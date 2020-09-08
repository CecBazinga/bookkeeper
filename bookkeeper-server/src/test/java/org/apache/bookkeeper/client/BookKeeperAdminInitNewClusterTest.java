package org.apache.bookkeeper.client;


import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;


import static org.junit.Assert.assertEquals;



@RunWith(value= Parameterized.class)
public class BookKeeperAdminInitNewClusterTest extends BookKeeperClusterTestCase {

    private boolean result;
    private ServerConfiguration conf;
    private String confType ;

    private static final int numOfBookies = 2;
    private final int lostBookieRecoveryDelayInitValue = 1800;

/*
    @Mock
    RegistrationManager mockedRM = mock(RegistrationManager.class) ;
*/


    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{

                //last parameter states if the method rm.initNewCluster() called inside
                // BookKeeperAdmin.initNewCluster(conf) must be mocked or not

                {true ,  "new" },
                {false , "null" },
                {false , "wrong"},
                //{false , "mock"},  //caso di test introdotto per portare la branch coverage al 100%
                                    // entrando nella clausola catch del metodo initNewCluste()

        });

    }



    public BookKeeperAdminInitNewClusterTest(boolean result , String conf) throws Exception {

        super(numOfBookies, 480);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);

        this.result = result;
        this.confType = conf;


    }

    @Test
    public void testInitNewCluster() throws Exception {

        boolean realResult ;


        if(confType == "null"){

            this.conf = null;

        }else if( confType == "wrong"){

            this.conf = new ServerConfiguration().setMetadataServiceUri("zk+hierarchical://127.0.0.1/ledgers");

        }else if(confType == "new") {

            this.conf = new ServerConfiguration(baseConf);
            String ledgersRootPath = "/testledgers";
            this.conf.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));

        }else if(confType == "mock"){



            this.conf = new ServerConfiguration(baseConf);
            String ledgersRootPath = "/testledgers";
            this.conf.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));


            //when(mockedRM.initNewCluster()).thenThrow(new Exception());



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