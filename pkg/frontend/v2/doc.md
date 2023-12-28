
# frontend v2

## framework

1. Establish Connection

    a. init

        init Session, WriteBuffers, PacketEndpoint.

    b. handshake

        handshake with the client.
        
        i. send Handshake packet.
        ii. receive HandshakeResponse packet.
            (a). (maybe) switch auth method for the password.
        iii. (maybe) switch to SSL/TLS.
            (a) establish TLS conn
        iv. auth user.
        v. send OKPacket.

    c. loop handle Mysql COMMAND

        loops on the conn:

            i. read payload from the client.
            ii. switch on the COMMAND

                case COM_QUERY:

                    doComQuery(query);

                case COM_XXX:

                    doComXXX();

2. Handle Query

    process the query from the client.

    doComQuery(query) {
        QueryExecutor.Exec(query)
    }

    QueryExecutor prepares something and executes the query.

    a. QueryExecutor.Open(...)

    b. QueryExecutor.Exec(query) {
        i. setup the process.Process.

        ii. parse the query.

            (maybe) hit the plan cache.

        iii. loop process the stmts.

            (a). check privilege on the statement level.

            (b). check the statement can be executed in the active transaction.

            (c). execute the statement.

                (A) (maybe) prepare the transaction

                (B) setup the statement executor
                
                    executor = xxxExecutor.Open(...)

                (C) run the statement executor

                    executor.Next(...)

                (D) (maybe) cleanup the transaction.
    }

3. Statement Executor

    the specific executor for the statement like DDL,DML,etc.
    the execution logic differs for different statements.
    but they does not differs so much. 
    there are two rules to seperate them:

        a. handling in the frontend or the computation engine.

            in the frontend:

                these statements does not need the query plans.

                i. some statements can be handled easily without the computation engine.
        
                    use, set,
                    begin, commit, rollback,
                    prepare, deallocate,
                    ShowVariables, ShowErrors, tree.ShowWarnings,
                    AnalyzeStmt, ExplainStmt, ShowBackendServers, Backup.

                ii. some statements can only be handled in the frontend.

                    MoDump, Connector related, DaemonTask related, 
                    Reset, ShowTableStatus, InternalCmdFieldList, 
                    publication/subscription related, stage related, 
                    UDF, store procedure, kill, load local.

                iii. account related statements.

                    SetRole,account, user,role,grant,revoke.

            in the computation engine:

                these statements needs the query plans and only can run in the computation engine.

                the rest of th statements that does exist above.

        b. response the result set or just status to the client
    
            response the result set:

                select, showXXX, explain, analyze, etc.

            response the status:

                begin,commit,rollback,set,etc.

    the executor has three methods:

        i. Open(...)

            prepare the transaction related, output related, etc.

        ii. Next(...)

            process the statement.

        iii. Close(...)


4. Transaction

    most of the statements need to be executed in the transaction.
    in the QueryExecutor.Open(...), it inits the transaction module.
    
    in the QueryExecutor.Exec(...),

        i. based on the Executor kind, it decides whether to start/commit a transaction.

        ii. in the Executor.Open(...), it uses the existed transaction. 

        iii. in the Executor.Next(...) and Executor.Close(...), 
            they do not create/drop/commit/rollback the transaction.

        iv. check the transaction state after the Executor ends.

            the transaction is replaced by a new one?

        iv. it commits/rollbacks the transaction if needed.
            it also may update the states of the transaction module.

5. logic in Executor.Next

    for different statement, the logic differs.

    the statement handled in the frontend:

        i. (maybe) rewrite statement first.

            analyze, explain analyze

        ii. execute the statement.

        iii. composite the result set packet or just status packet.

        iv. send the packet to the client.

    the statement handled in the computation engine:

        i. build plan for the statement.

        ii. compile.Compile.

        iii. compile.Run.

            (a). start the pipeline

            (b). pipeline outputs results.

                the writers writes the result rows to different destinations.

        iv. sends end packet to the client.

6. logic in the background executor

    the background executor is almost the same as the query executor.
    but, there is no client conn, no packet endpoint,session, logging in it, etc.
    there are multiple categories of the background executor


    rules to seperate them :

    (i). share the same transaction or not.

        it needs an txn before execution.

    (ii). outputs the binary result set or text result set or just status.



## input data

1. MysqlReadPacket

    the data from the client is decoded into different MysqlReadPacket.

    Command: the command type of the packet. sent by the client after the authentication succeeds.

    MysqlReadPacket: the handshake iteration reads the packet directly from the client.

    ```
                TCPConn
                    |
                    |
                decoder
                    |
                |---|-----------|
                |               |
            Command          MysqlReadPacket
    ```

## output data

1. ChunksWriter
    
    directly output the data from the computation engine into different destination.

2. MysqlFormatWriter

    Application: interation with the mysql client or jdbc.

    write the data into mysql client.
    
    first, it converts the column layout to the row layout.
    second, it reorganizes the row to the MysqlWritePacket.
    third, PacketEndPoint sends the MysqlWritePacket to the client. 
        Internally, the packet is encoded using mysql packaging rules.

    MysqlPayloadWriteBuffer splits the package into multiple payloads following
        the mysql payload rules.

3. CsvFormatWriter

    Application: select ... into outfile or stage.

    write the data into csv file. the file can be stored in the local file system
        or in the s3.

    first, it converts the column layout to the row layout.
    second, it encoding the row to the csv format and saves them.

4. S3Writer

    Application: save query result

    write the chunks and the meta into the s3.


    ```
                                        ChunksWriter
                                            |
                |---------------------------|----------------------------
                |                               |                   |
                |                               |                   |
        MysqlFormatWriter                   CsvFormatWriter        S3Writer
                |                               |                   |
                |                               |                   |
        PacketEndPoint,MysqlWritePacket         |                   |
                |                               |                   |
        MysqlPayloadWriteBuffer                 |                   |
                |                               |                   |
                |                               |                   |
                |                               |                   |
                |                               |                   |
            TCPConn                       Fileservice            Fileservice
    ```