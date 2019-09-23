package resources

/*
 * I tried using both Spock to mock a runnable, but I couldn't get it to serialize correctly
 * I also tried using Mockito but couldn't get it work either. However, it is likely that
 * my ultimate solution of compiling the test classes would have worked with Mockito
 */
class MockRunnable implements Runnable, Serializable {

    private Long callCount = 0
    private ArrayList<Long> delayTimes = []
    private lastCallTime = System.currentTimeMillis()

    @Override
    void run () {
        //increment call count
        callCount++
        Long callTime = System.currentTimeMillis()
        //store call time
        delayTimes << callTime - lastCallTime
        lastCallTime = callTime
    }

    Long getCallCount() {
        return callCount
    }

    ArrayList<Long> getDelayTimes() {
        return delayTimes
    }
}