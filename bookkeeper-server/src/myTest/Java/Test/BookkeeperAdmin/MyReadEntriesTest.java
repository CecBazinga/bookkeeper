package Test.BookkeeperAdmin;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BookKeeperAdmin.*;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.InjectMocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;



@RunWith(value= Parameterized.class)
public class MyReadEntriesTest extends BookKeeperClusterTestCase {

    private String result;
    private long ledgerId ;
    private long firstEntry ;
    private long lastEntry ;

    private BookKeeperAdmin bA;
    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperAdminTest.class);
    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final int numOfBookies = 2;
    private final int lostBookieRecoveryDelayInitValue = 1800;



    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters()  {
        return Arrays.asList(new Object[][]{

                {"no",  -1 , -1 , -1},
                {"yes",  0 ,  1 ,  2},
                {"yes",  1 ,  0 , -1},

        });

    }



    public MyReadEntriesTest(String result , long ledgerId , long firstEntry , long lastEntry){

        super(numOfBookies, 480);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);


        this.result = result;
        this.ledgerId = ledgerId;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;

    }

    @Test
    public void readEntriesTest() {

        try {
            this.bA = new BookKeeperAdmin(baseClientConf);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }

        //mi aspetto che il metodo sia funzionamente e mi venga
        // ritornato un oggetto di tipo LedgerEntriesIterable



        if(result == "yes"){

            try {

                LedgerEntriesIterable expectedResult = bA.new LedgerEntriesIterable(ledgerId,firstEntry,lastEntry);
                LedgerEntriesIterable realResult = (LedgerEntriesIterable) bA.readEntries(ledgerId,firstEntry,lastEntry);

                assertThat(realResult, samePropertyValuesAs(expectedResult));


            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BKException e) {
                e.printStackTrace();
            }

        //in questo caso mi aspetto che vengano sollevate delle eccezioni
        }else{

            boolean expectedBoolean = true ;
            boolean exceptionHasBeenRaised = false ;
            try {

                Iterable<LedgerEntry> realResult = bA.readEntries(ledgerId,firstEntry,lastEntry);

            } catch (Exception e) {
                exceptionHasBeenRaised = true ;
                e.printStackTrace();
            }

            assertEquals(expectedBoolean , exceptionHasBeenRaised);

        }

    }
}
