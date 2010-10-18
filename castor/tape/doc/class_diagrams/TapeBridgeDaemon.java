/**
 */
class BaseDaemon {
}


/**
 * @assoc 1 - 1 DriveAllocationProtocolEngine
 * @assoc 1 - 1 BridgeProtocolEngine
 */
class VdqmRequestHandler {
}


/**
 * @composed 1 - 1 VdqmRequestHandler
 */
class TCPListenerThreadPool {
}


/**
 * @opt operations
 */
class DriveAllocationProtocolEngine {
public void run();
}


/**
 * @opt operations
 */
class BridgeProtocolEngine {
public void run();
}


/**
 * @composed 1 - 1 TCPListenerThreadPool
 */
class TapeBridgeDaemon extends BaseDaemon {
}
