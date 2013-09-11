BpTree APIs
===========

BpTree
------

    /**
     * @constructor
     * @param {Number} order Must be a positive integer greater than 2
     * @param {Function} keyCmp A function to compare keys; see below
     * @param {Store} store
     */
    bpt = new BpTree( order, keyCmp, store)
    
    /**
     * @param {Object} k Key value
     * @param {Object} v Value value ?!? :}
     * @param {Function} cb cb(err)
     */
    bpt.put( k, v, cb )
    
    /**
     * @param {Object} k Key to retrieve value for
     * @param {Function} cb cb(err, value)
     */
    bpt.get( k, cb )

Key & keyCmp
------------

The key is any object that can be encoded into a buffer via `msgpack-js`.
The `keyCmp` function takes two keys and comares them resulting is -1, 0, or 1
representing less-than, equal, or greater-than relationship.

/**
 * @param {Object} a
 * @param {Object} b
 * @return {Number} n -1 (less-than), 0 (equals), 1 (greater-than)
 */
n = keyCmp( a, b ) /* n == 0 || 1 || -1 */

Value
-----
The key is any object that can be encoded into a buffer via `msgpack-js`.

Handle
------

    /**
     * Test if this Handle equals another
     *	 
     * @param {Handle} other
     * @return {Boolean}
     */
     bool = hdl.equals( other )


    /**
     * Create a unique string representation of the Handle
     *
     * @return {String}
     */
    str = hdl.toString()

Store
-----

    /**
     * Store.Handle is a Handle compliant constructor
     */
    Handle = Store.Handle
    
    /**
     * Reserve a handle for a given sized buffer.
     *
     * @param {Number} sz an integer number of bytes the handle must represent
     * @param {Function} cb cb(err, hdl)
     */
    store.reserve( sz, cb )
    
    /**
     * Release a handle space from storage.
     * Trivial in the case of MemStore
     *
     * @param {Handle} hdl
     * @param {Function} cb cb(err)
     */
    store.release( hdl, cb )

    /**
     * Convert a Handle to a storable plain ole JSON object
     *
     * @param {Handle} hdl
     * @return {Object} plain ole JSON object
     */
    json = store.handleToJSON( hdl )

    /**
     * Convert a plain ole JSON object to a Handle
     *
     * @param {Object} json plain ole JSON object
     * @return {Handle}
     */
    hdl = store.handleFromJSON( json )
    
    /**
     * Load a buffer for a given Handle
     *
     * @param {Handle} hdl
     * @param {Function} cb cb(err, buf)
     */
    store.load( hdl, cb )
    
    /**
     * Store a buffer in a give Handle
     *
     * @param {Buffer} buf
     * @param {Handle} [hdl]
     * @param {Function} cb cb(err, hdl)
     */
    store.store( buf, hdl, cb )

    /**
     * Flush out all data to disk
     *
     * @param {Function} cb cb(err)
     */
    store.flush(cb)

    /**
     *	Close the underlying store
     *
     * @param {Function} cb cb(err)
     */
    store.close(cb)

### THE END
