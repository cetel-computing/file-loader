Imports System.Globalization
Imports System.Data.Odbc
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Text
Imports System.Threading
Imports System.IO.Compression


Public Class Events

    Public Shared Function InsertFileLogging(ByVal ClientName As String, ByVal EventID As String, ByVal FileDirectory As String, ByVal FileName As String, _
                                    ByVal TableToLoad As String, ByVal Message As String, ByVal IsError As Integer)
        Dim Sql As String = ""

        Try

            Sql = "INSERT INTO " + ClientName + "_db.fl2_file_logging " +
                  "( " +
                  "    fl_id " +
                  "    ,Event_id " +
                  "    ,File_directory " +
                  "    ,File_name " +
                  "    ,Table_toload " +
                  "    ,Message " +
                  "    ,Error " +
                  "    ,Create_date " +
                  ") " +
                  "SELECT (SELECT MAX(fl_id) FROM " + ClientName + "_db.fl2_file_logging)+1 AS file_key," +
                  "     '" + EventID + "', " +
                  "     '" + FileDirectory + "', " +
                  "     '" + FileName + "', " +
                  "     '" + TableToLoad + "', " +
                  "     '" + Message + "', " +
                  "      " + IsError.ToString + " , " +
                  "     CURRENT_TIMESTAMP "

            Using MyConn As New OdbcConnection(ConnectionString)
                Dim MyComm As New OdbcCommand
                MyConn.Open()

                Try

                    MyComm.Connection = MyConn
                    MyComm.CommandText = Sql
                    MyComm.ExecuteNonQuery()

                Catch ex As OdbcException

                    Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in InsertFileLogging", "Error in Automated File Loader, please check" + Environment.NewLine + Environment.NewLine + ex.Message.ToString + Environment.NewLine + Environment.NewLine + Sql, "", Command, False, EmailTable)

                    Return False

                End Try

            End Using

            Return True

        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in InsertFileLogging", "Error in Automated File Loader, please check" + Environment.NewLine + Environment.NewLine + ex.Message.ToString + Environment.NewLine + Environment.NewLine + Sql, "", Command, False, EmailTable)
            Return False

        End Try

    End Function

    Public Shared Function InsertFileLoadLog(ByVal ClientName As String, ByVal EventID As String, ByVal FileDirectory As String, ByVal FileName As String, _
                                    ByVal TableToLoad As String, ByVal Message As String)
        Dim Sql As String = ""

        Try

            Sql = "INSERT INTO " + ClientName + "_db.fl2_file_load_log " +
                  "( " +
                  "     fll_id " +
                  "    ,file_key " +
                  "    ,File_directory " +
                  "    ,File_name " +
                  "    ,Table_toload " +
                  "    ,records_loaded " +
                  "    ,Create_date " +
                  ") " +
                  "SELECT  (SELECT MAX(fll_id) FROM " + ClientName + "_db.fl2_file_load_log)+1 AS fll_id," +
                  "     '" + EventID + "', " +
                  "     '" + FileDirectory + "', " +
                  "     '" + FileName + "', " +
                  "     '" + TableToLoad + "', " +
                  "     " + Message + ", " +
                  "          CURRENT_TIMESTAMP "

            Using MyConn As New OdbcConnection(ConnectionString)
                Dim MyComm As New OdbcCommand
                MyConn.Open()

                Try

                    MyComm.Connection = MyConn
                    MyComm.CommandText = Sql
                    MyComm.ExecuteNonQuery()

                Catch ex As OdbcException

                    Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in InsertFileLoadLog", "Error in Automated File Loader, please check" + Environment.NewLine + Environment.NewLine + ex.Message.ToString + Environment.NewLine + Environment.NewLine + Sql, "", Command, False, EmailTable)

                    Return False

                End Try

            End Using

            Return True

        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in InsertFileLoadLog", "Error in Automated File Loader, please check" + Environment.NewLine + Environment.NewLine + ex.Message.ToString + Environment.NewLine + Environment.NewLine + Sql, "", Command, False, EmailTable)
            Return False

        End Try

    End Function

    Public Shared Function DeleteFileLoadLog(ByVal ClientName As String, ByVal EventID As String, ByVal FileName As String)
        Dim Sql As String = ""

        Try

            Sql = "DELETE " +
                  "FROM  " + ClientName + "_db.fl2_file_load_log " +
                  "WHERE fll_id in (SELECT max(fll_id) FROM " + ClientName + "_db.fl2_file_load_log WHERE file_key = '" + EventID + "' AND File_name = '" + FileName + "' ) "

            Using MyConn As New OdbcConnection(ConnectionString)
                Dim MyComm As New OdbcCommand
                MyConn.Open()

                Try

                    MyComm.Connection = MyConn
                    MyComm.CommandText = Sql
                    MyComm.ExecuteNonQuery()

                Catch ex As OdbcException

                    Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in DeleteFileLoadLog", "Error in Automated File Loader, please check" + Environment.NewLine + Environment.NewLine + ex.Message.ToString + Environment.NewLine + Environment.NewLine + Sql, "", Command, False, EmailTable)

                    Return False

                End Try

            End Using

            Return True

        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in DeleteFileLoadLog", "Error in Automated File Loader, please check" + Environment.NewLine + Environment.NewLine + ex.Message.ToString + Environment.NewLine + Environment.NewLine + Sql, "", Command, False, EmailTable)
            Return False

        End Try

    End Function


    Public Function LoadDataToStaging(ByVal LoadParams As DataTable, ByVal FullFileDirectory As String, ByVal FileToLoad As String) As Boolean
        Dim WxLoaderCode As String = ""
        Dim RecordCount As Integer = 0
        Dim LogFile As String = ""
        Dim ErrorFile As String = ""

        Dim DataRow As DataRow = LoadParams.Rows(0)

        Dim LoadType As String = DataRow("staging_table_name").Split(".")(1)

        LogFile = DataRow("wxloader_directory") + "Logs\" + DataRow("file_key").ToString + "_" + LoadType.ToString.ToLower + "_" + Today.ToShortDateString.Replace("/", "") + "_" + DateTime.Now.ToString("HHmmss") + ".log"
        ErrorFile = DataRow("wxloader_directory") + "Logs\" + DataRow("file_key").ToString + "_" + LoadType.ToString.ToLower + "_" + Today.ToShortDateString.Replace("/", "") + "_" + DateTime.Now.ToString("HHmmss") + ".err"

        WxLoaderCode = DataRow("wxloader_statement").ToString

        WxLoaderCode = WxLoaderCode.Replace(ControlChars.CrLf, "")
        WxLoaderCode = WxLoaderCode.Replace(ControlChars.Cr, "")
        WxLoaderCode = WxLoaderCode.Replace(ControlChars.Lf, "")

        WxLoaderCode = Microsoft.VisualBasic.Strings.Replace(WxLoaderCode, "<dsn>", Replace(DSN, "32", "64"), 1, -1, Constants.vbTextCompare)
        WxLoaderCode = Microsoft.VisualBasic.Strings.Replace(WxLoaderCode, "<uid>", UID, 1, -1, Constants.vbTextCompare)
        WxLoaderCode = Microsoft.VisualBasic.Strings.Replace(WxLoaderCode, "<pwd>", PWD, 1, -1, Constants.vbTextCompare)
        WxLoaderCode = Microsoft.VisualBasic.Strings.Replace(WxLoaderCode, "<table>", DataRow("staging_table_name").ToString, 1, -1, Constants.vbTextCompare)
        WxLoaderCode = Microsoft.VisualBasic.Strings.Replace(WxLoaderCode, "<err_file>", ErrorFile.ToString, 1, -1, Constants.vbTextCompare)
        WxLoaderCode = Microsoft.VisualBasic.Strings.Replace(WxLoaderCode, "<log_file>", LogFile.ToString, 1, -1, Constants.vbTextCompare)
        WxLoaderCode = Microsoft.VisualBasic.Strings.Replace(WxLoaderCode, "<file>", DataRow("full_path_toload").ToString, 1, -1, Constants.vbTextCompare)

        Try

            If RunCommand(WxLoaderCode, DataRow("file_key").ToString, LogFile, ErrorFile.ToString, FileToLoad.ToString, DataRow("staging_table_name").ToString) Then

                RecordCount = GetLoadedCounts(DataRow("staging_table_name").ToString, DataRow("file_key").ToString)

                InsertFileLogging(ClientName, DataRow("file_key").ToString, FullFileDirectory, FileToLoad, DataRow("staging_table_name").ToString, "Loaded : " & RecordCount & " Records ", 0)
                InsertFileLoadLog(ClientName, DataRow("file_key").ToString, FullFileDirectory, FileToLoad, DataRow("staging_table_name").ToString, RecordCount.ToString)

            Else

                Try

                    Dim Dr As DataRow = ErroredFiles.NewRow
                    Dr("ErroredFileName") = FileToLoad.ToString
                    Dr("LogFileDirectory") = LogFile.ToString
                    Dr("ErrorFileDirectory") = ErrorFile.ToString
                    ErroredFiles.Rows.Add(Dr)

                Catch ex As Exception

                End Try

                Return False

            End If

        Catch ex As Exception
            InsertFileLogging(ClientName, DataRow("file_key").ToString, FullFileDirectory, FileToLoad, DataRow("staging_table_name").ToString, "Error Loading data to the staging table", 1)
            Throw New Exception("Error Occured in LoadDataToStaging : " + ex.Message.ToString)
        End Try

        Return True

    End Function

    Private Function RunCommand(WxLoaderCode As String, ByVal EventId As String, ByVal LogFile As String, ByVal ErrorFile As String, ByVal FileToLoad As String, ByVal TableName As String) As Boolean
        Dim Args As String = ""
        Dim Output As String = ""
        Dim ErrorOutput As String = ""
        Dim StandardOutput As String = ""
        Dim RecordsLoaded As String = ""
        Dim RecordsRejected As String = "X"

        Try

            Dim cmdProcess As New Process
            With cmdProcess
                .StartInfo = New ProcessStartInfo("cmd.exe")
                With .StartInfo
                    .CreateNoWindow = True
                    .UseShellExecute = False
                    .RedirectStandardOutput = True
                    .RedirectStandardError = True
                    .Arguments = String.Format("/k " + WxLoaderCode)
                End With
                .Start()
                .WaitForExit(5000)     '------ 15 seconds
            End With

            Try
                cmdProcess.Kill()
                Output = cmdProcess.StandardOutput.ReadToEnd
                ErrorOutput = cmdProcess.StandardError.ReadToEnd
            Catch ex As Exception
                'already dead, let it go
                Output = cmdProcess.StandardOutput.ReadToEnd
                ErrorOutput = cmdProcess.StandardError.ReadToEnd
            End Try

            Dim Parts As String() = Regex.Split(Output, vbCr)

            If Output.ToString.Contains("rolled back") Then

                InsertFileLogging(ClientName, EventId, "", "", "", "Error In Run WXLoader : " + Output.ToString + " ----- WxLoader string is : " + WxLoaderCode.ToString, 1)

                RecordsLoaded = 0

                Return False

            ElseIf Output.ToString.Contains("rejected") Then

                Try

                    RecordsRejected = Parts(5).ToString()
                    RecordsRejected = RecordsRejected.Substring((InStr(RecordsRejected, "rejected") - 6), 5).Trim()

                Catch ex As Exception
                    'during testing for record loaded count, fails on errors
                End Try

                Try

                    Dim Dr As DataRow = RejectedRecordsFiles.NewRow
                    Dr("RejectedFileName") = FileToLoad.ToString
                    Dr("NumberOfRecords") = RecordsRejected
                    Dr("LogFileDirectory") = LogFile.ToString
                    Dr("ErrorFileDirectory") = ErrorFile.ToString
                    RejectedRecordsFiles.Rows.Add(Dr)

                Catch ex As Exception

                End Try

            End If

        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in Run Load Command", "Error in Automated File Loader, please check" + "<BR/>" + "<BR/>" + ex.Message.ToString, LogFile, Command, False, EmailTable)

            InsertFileLogging(ClientName, EventId, "", "", "", "Error In Run WXLoader : " + ex.Message.ToString, 1)

            Return False

        End Try

        Return True

    End Function

    Public Function MoveFiles(ByVal FileToMove As String, ByVal FileDirectory As String, ByVal EventID As String, ByVal DevRun As Boolean, Optional ByVal ActiveFile As Boolean = True) As Boolean
        Dim CurrentDirectory As String = ""
        Dim CurrentFile As String = ""
        Dim ArchiveDirectory As String = ""
        Dim ArchiveZip As String = ""
        Dim InActiveFileDirectory As String = ""

        Try

            CurrentDirectory = FileToMove.Substring(0, FileToMove.LastIndexOf("\") + 1)
            CurrentFile = FileToMove.Substring(FileToMove.LastIndexOf("\") + 1, (FileToMove.Length - FileToMove.LastIndexOf("\") - 1))
            ArchiveDirectory = String.Format(FileDirectory + "Archive\" + CurrentFile)
            ArchiveZip = String.Format(FileDirectory + "Archive\" + DateTime.Now.ToString("yyyyMMdd") + ".zip")
            InActiveFileDirectory = String.Format(FileDirectory + "InactiveFiles\" + CurrentFile)

            Console.WriteLine("Moving File : " + CurrentFile)

        Catch ex As Exception

            Return False

        End Try

        Try
            If ActiveFile = False Then

                If File.Exists(CurrentDirectory + CurrentFile) = False Then
                    Dim fs As FileStream = File.Create(CurrentDirectory + CurrentFile)
                    fs.Close()
                End If

                ' Ensure that the file does not exist. means I can run more than once
                If File.Exists(InActiveFileDirectory) Then
                    File.Delete(InActiveFileDirectory)
                End If

                ' Move the file.
                If DevRun Then
                    File.Copy(CurrentDirectory + CurrentFile, InActiveFileDirectory)
                Else
                    File.Move(CurrentDirectory + CurrentFile, InActiveFileDirectory)
                End If

                InsertFileLogging(ClientName, EventID, CurrentDirectory, InActiveFileDirectory, "", "File Moved to InActiveFiles Folder ", 0)

                Return True

            Else

                If File.Exists(CurrentDirectory + CurrentFile) = False Then
                    Dim fs As FileStream = File.Create(CurrentDirectory + CurrentFile)
                    fs.Close()
                End If

                ' Ensure that the file does not exist. means I can run more than once
                If File.Exists(ArchiveDirectory) Then
                    File.Delete(ArchiveDirectory)
                End If

                ' Move the file.
                If DevRun Then
                    File.Copy(CurrentDirectory + CurrentFile, ArchiveDirectory)
                Else
                    File.Move(CurrentDirectory + CurrentFile, ArchiveDirectory)
                End If

                Try

                    Using archive As ZipArchive = ZipFile.Open(ArchiveZip, ZipArchiveMode.Update)

                        archive.CreateEntryFromFile(ArchiveDirectory, CurrentFile)

                    End Using

                Catch ex As Exception

                    'cant zip the moved file for some reason - noramlly out of memory - just exit...
                    Return True

                End Try

                File.Delete(ArchiveDirectory)

                Return True

            End If

        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in MoveFiles", "Error in Automated File Loader, Error moving files " + "<BR/>" + "<BR/>" + ex.Message.ToString, "", Command, False, EmailTable)

            InsertFileLogging(ClientName, EventID, CurrentDirectory, ArchiveDirectory + FileToMove, "", "Error Moving File to Archive : " + ex.Message.ToString, 1)

            Return False

        End Try

    End Function

    Private Function GetLoadedCounts(ByVal TableName As String, ByVal EventID As String) As Integer
        Dim NoOfRecords As Integer = 0
        Dim RecordsTable As New DataTable
        Dim Sql As String = ""

        Try

            Sql = " SELECT COUNT(*) " +
                    "FROM " + TableName + " " +
                    "AT NOW"

            Using connection As New OdbcConnection(ConnectionString)
                Dim adapter As New OdbcDataAdapter(Sql, connection)

                connection.Open()
                adapter.Fill(RecordsTable)

            End Using

            NoOfRecords = RecordsTable.Rows(0).Item(0).ToString

            Return NoOfRecords

        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in GetLoadedCounts", "Error in Automated File Loader, failed to get loaded counts from table" + "<BR/>" + "<BR/>" + ex.Message.ToString, "", Command, False, EmailTable)

            Return 0

        End Try

    End Function

    Public Function GetDataBaseDetailsForFile(ByVal FullFileDirectory As String, ByVal Filename As String, ByVal ClientName As String, ByVal DevRun As Boolean) As DataTable
        Dim FileLoadDetails As New DataTable
        Dim EmptyTable As New DataTable
        Dim SearchFile As String = ""
        Dim FileSql As String = ""

        SearchFile = Regex.Replace(Filename.ToLower.Replace(".csv", "").Replace(".txt", "").Replace(".dat", "").Replace(".xlsx", "").Replace(".xls", ""), "[\d]", "1").Trim

        FileSql = "SELECT file_key " +
                "        ,file_name " +
                "        ,set_key " +
                "        ,zip_file " +
                "        ,staging_table_name " +
                "        ,case when right(trim(wxloader_directory),1) = '\' then trim(wxloader_directory) else trim(wxloader_directory)||'\' end as wxloader_directory " +
                "        ,lower(file_name_date_pattern) as file_name_date_pattern" +
                "        ,wxloader_statement " +
                "        ,active " +
                "        ,'" + FullFileDirectory.ToString.Replace(".xlsx", ".txt").Replace(".xls", ".txt") + "' AS full_path_toload " +
                "FROM    " + ClientName.ToString + "_db.fl2_file_feed_details " +
                "WHERE   LOWER(File_name) ILIKE LOWER('" + SearchFile.ToString + "%')" +
                "ORDER BY active desc, create_date desc"

        Using connection As New OdbcConnection(ConnectionString)
            Dim adapter As New OdbcDataAdapter(FileSql, connection)

            Try

                connection.Open()
                adapter.Fill(FileLoadDetails)

            Catch ex As OdbcException

                InsertFileLogging(ClientName, "", "", "", "", "Error in GetDataBaseDetailsForFile" + ex.Message.ToString, 1)

                Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in GetDataBaseDetailsForFile", "Error in Automated File Loader, Error : " + "<BR/>" + "<BR/>" + ex.Message.ToString, "", "", False, EmailTable)

                Return EmptyTable

            End Try

        End Using

        If FileLoadDetails.Rows.Count >= 1 AndAlso FileLoadDetails.Rows(0).Item("active").ToString = 0 Then
            'if there is only inactive records then error
            ActiveFile = False

            MoveFiles(FullFileDirectory, FileLoadDetails.Rows(0).Item("wxloader_directory").ToString, FileLoadDetails.Rows(0).Item("file_key").ToString, DevRun, False)

            Return EmptyTable

        ElseIf FileLoadDetails.Rows.Count > 1 Then
            'if there is more than 1 record, but the first 1 is active, then log it, but continue to process

            InsertFileLogging(ClientName, "", "", "", "", "Error in GetDataBaseDetailsForFile : More than one instance of the file in the setup table for the filename : " + Filename.ToString, 1)

            Return FileLoadDetails

        ElseIf FileLoadDetails.Rows.Count = 0 Then

            UnknownFile = True

            LogUnknownFile(SearchFile.ToString, FullFileDirectory, Filename, DevRun)

            Return EmptyTable

        Else

            ActiveFile = True

            Return FileLoadDetails

        End If

    End Function


    Private Shared Sub LogUnknownFile(ByVal SearchFileName As String, ByVal FullFileDirectory As String, ByVal Filename As String, ByVal DevRun As Boolean)
        Dim CurrentDirectory As String = ""
        Dim CurrentFile As String = ""
        Dim UnknownDirectory As String = ""
        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""

        InsertFileLogging(ClientName, "", "Automated File Loader", "Automated File Loader", "", "Unknown file found on the FTP site" + Filename.ToString, 2)

        Try
            'Add filename to table ready for output at the end
            Dim Dr As DataRow = UnknownFiles.NewRow
            Dr("UnknownFileName") = Filename.ToString
            UnknownFiles.Rows.Add(Dr)

        Catch ex As Exception

        End Try

        Try

            CurrentDirectory = FullFileDirectory.Substring(0, FullFileDirectory.LastIndexOf("\") + 1)
            CurrentFile = FullFileDirectory.Substring(FullFileDirectory.LastIndexOf("\") + 1, (FullFileDirectory.Length - FullFileDirectory.LastIndexOf("\") - 1))
            UnknownDirectory = "\Data\Clients\" + ClientUnknownDirectory.ToString + "\data_in\UnknownFiles\" + CurrentFile

        Catch ex As Exception

        End Try

        Try
            If File.Exists(CurrentDirectory + CurrentFile) = False Then
                Dim fs As FileStream = File.Create(CurrentDirectory + CurrentFile)
                fs.Close()
            End If

            ' Ensure that the file does not exist. means I can run more than once
            If File.Exists(UnknownDirectory) Then
                File.Delete(UnknownDirectory)
            End If

            ' Move the file.
            If DevRun Then
                File.Copy(CurrentDirectory + CurrentFile, UnknownDirectory)
            Else
                File.Move(CurrentDirectory + CurrentFile, UnknownDirectory)
            End If


        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in MoveFilesToUnknown File Directory", "Error in Automated File Loader, unable to move file : " + CurrentFile.ToString + "To : " + UnknownDirectory + "<BR/>" + "<BR/>" _
                        + Environment.NewLine + Environment.NewLine + "Error Message : " + ex.Message.ToString, "", Command, False, EmailTable)

        End Try

    End Sub

    Public Function MapDataToWarehouse(ByVal EventID As String, ByVal FileToProcessData As DataTable, ByVal CurrentFileDate As Date, Optional ByVal FileName As String = "", Optional ByVal StageTableName As String = "")

        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""
        Dim Directory As String = ""

        Dim FileMappingSql As New DataTable
        Dim MappingSqlResult As New DataTable
        Dim FileSql As String = ""

        'get all the sql mapping to run for this file
        FileSql = "SELECT mapping_sql, result_is_dynamic_sql, ignore_error " +
                "FROM  " + ClientName.ToString + "_db.fl2_file_feed_mapping " +
                "WHERE LOWER(file_key) = LOWER('" + EventID.ToString + "') AND active = 1 " +
                "ORDER By run_order"

        Using connection As New OdbcConnection(ConnectionString)

            Dim adapter As New OdbcDataAdapter(FileSql, connection)

            Try
                connection.Open()
                adapter.Fill(FileMappingSql)

            Catch ex As OdbcException
                'currently only set up for Nestle.  If no table return as still missing
                Return "Missing Warehouse Load Mapping"

            End Try

            If FileMappingSql.Rows.Count = 0 Then
                'if no rows then return missing
                Return "Missing or Inactive Warehouse Load Mapping"

            End If

            'if we have some mapping sql scripts then run through them in order
            For Each row As DataRow In FileMappingSql.Rows

                Dim Result As Integer = 0
                Dim cmd As New OdbcCommand
                Dim Sql As String = ""

                Sql = row("mapping_sql")
                'replace stuff in the sql

                Sql = Sql.Replace(ControlChars.CrLf, " ")
                Sql = Sql.Replace(ControlChars.Cr, " ")
                Sql = Sql.Replace(ControlChars.Lf, " ")

                Sql = Microsoft.VisualBasic.Strings.Replace(Sql, "<filename>", FileName.ToString, 1, -1, Constants.vbTextCompare)
                Sql = Microsoft.VisualBasic.Strings.Replace(Sql, "<filedate>", CurrentFileDate.ToString("yyyy-MM-dd"), 1, -1, Constants.vbTextCompare)
                Sql = Microsoft.VisualBasic.Strings.Replace(Sql, "<stagetablename>", StageTableName.ToString, 1, -1, Constants.vbTextCompare)

                If row("result_is_dynamic_sql") = "1" Then

                    Dim ResultAdapter As New OdbcDataAdapter(Sql, connection)

                    Try
                        'connection.Open()
                        ResultAdapter.Fill(MappingSqlResult)

                    Catch ex As OdbcException

                        Return "Error in File Loader writing Data to Warehouse table. Sql run: " + Sql

                    End Try

                    Dim NewSql As String = ""

                    For Each dataRow As DataRow In MappingSqlResult.Rows

                        NewSql = NewSql + dataRow(0).ToString + " "

                    Next

                    Try
                        cmd.Connection = connection
                        cmd.CommandText = NewSql
                        cmd.ExecuteNonQuery()

                    Catch ex As Exception
                        If row("ignore_error") = "1" Then

                            Exit Try

                        Else

                            Return "Error in File Loader writing Data to Warehouse table. Sql run: " + NewSql

                        End If

                    End Try

                Else
                    Try
                        cmd.Connection = connection
                        cmd.CommandText = Sql
                        cmd.ExecuteNonQuery()

                    Catch ex As Exception

                        If row("ignore_error") = "1" Then

                            Exit Try

                        Else

                            Return "Error in File Loader writing Data to Warehouse table. Sql run: " + Sql

                        End If

                    End Try
                End If

            Next

        End Using

        'assume everything is good if it all ran...
        Return "True"


    End Function

    Public Function CreateTableImages(ByVal ClientName As String, ByVal FilesToProcess As DataTable) As Boolean
        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""
        Dim Directory As String = ""
        Dim Sql As String = ""
        Dim TableList As New DataTable
        Dim ForTableName As String = ""
        Dim CreateImage As Boolean = False
        Dim TruncateTable As Boolean = False


        If FilesToProcess.Rows.Count > 0 Then

            Dim query = _
                From order In FilesToProcess.AsEnumerable() _
                Where order.Field(Of Integer)("TotalRows") > 1 _
                Select order

            If query.Count = 0 Then

                Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in CreateTableImages", "Error in Automated File Loader, There appears to be files to process but no data in any of them. " + "<BR/>" + "<BR/>" + "Please check the files and reload if neccessary.", "", "", False, EmailTable)

                Environment.Exit(-1)

            End If

            Dim ImagesToCreate As DataTable = query.CopyToDataTable()

            Dim FilesList As String = String.Join("','", (From row In ImagesToCreate.AsEnumerable Select row("FileNamePattern").ToString.ToLower).ToArray)

            Sql = "SELECT DISTINCT lower(table_name) as table_name, create_image, truncate_table " +
                    "FROM " + ClientName.ToString + "_db.fl2_table_options "

            Sql = Sql + "UNION " +
                        "SELECT '" + ClientName.ToString + "_db.fl2_file_feed_details' as table_name, 1 as create_image, 0 as truncate_table " +
                        "UNION " +
                        "SELECT '" + ClientName.ToString + "_db.fl2_file_feed_mapping' as table_name, 1 as create_image, 0 as truncate_table " +
                        "UNION " +
                        "SELECT '" + ClientName.ToString + "_db.fl2_file_feed_validation' as table_name, 1 as create_image, 0 as truncate_table " +
                        "UNION " +
                        "SELECT '" + ClientName.ToString + "_db.fl2_file_load_log' as table_name, 1 as create_image, 0 as truncate_table " +
                        "UNION " +
                        "SELECT '" + ClientName.ToString + "_db.fl2_file_loader_emails' as table_name, 1 as create_image, 0 as truncate_table " +
                        "UNION " +
                        "SELECT '" + ClientName.ToString + "_db.fl2_file_logging' as table_name, 1 as create_image, 0 as truncate_table " +
                        "ORDER BY truncate_table DESC"

            Using connection As New OdbcConnection(ConnectionString)
                Dim adapter As New OdbcDataAdapter(Sql, connection)

                Try

                    connection.Open()
                    adapter.Fill(TableList)

                Catch ex As OdbcException

                    InsertFileLogging(ClientName, "", "", "", "", "Error in CreateTableImages - Get Tables Failed" + ex.Message.ToString, 1)

                    EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error in CreateTableImages"
                    EmailBody = " Error in Automated File Loader, Error in CreateTableImages : " + "<BR/>" + "<BR/>"
                    EmailBody += ex.Message.ToString

                    Email.SendEMail(EmailSubject, EmailBody, "", "", False, EmailTable)

                    Return False

                End Try

            End Using

            Using MyConn As New OdbcConnection(ConnectionString)
                Dim MyComm As New OdbcCommand
                MyConn.Open()
                MyComm.Connection = MyConn

                For Each row As DataRow In TableList.Rows

                    ForTableName = row("table_name")
                    CreateImage = row("create_image")
                    TruncateTable = row("truncate_table")

                    If CreateImage Then
                        Sql = "CREATE OR REPLACE TABLE IMAGE " + ForTableName.ToString
                    ElseIf TruncateTable Then
                        Sql = "TRUNCATE TABLE " + ForTableName.ToString
                    End If

                    Try

                        MyComm.CommandText = Sql
                        MyComm.ExecuteNonQuery()

                    Catch ex As Exception

                        EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error in CreateTableImages"
                        EmailBody = " Error in Automated File Loader, Error in CreateTableImages : " + "<BR/>" + "<BR/>"
                        EmailBody += ex.Message.ToString

                        Email.SendEMail(EmailSubject, EmailBody, "", "", False, EmailTable)

                    End Try

                Next

            End Using

        End If

        Return True

    End Function

    Public Function DropTableImages(ByVal ClientName As String, ByVal FilesToProcess As DataTable) As Boolean
        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""
        Dim Directory As String = ""
        Dim Sql As String = ""
        Dim TableList As New DataTable
        Dim ForTableName As String = ""
        Dim DropImage As Boolean = False
        Dim TruncateTable As Boolean = False

        If Not FilesToProcess.Rows.Count = 0 Then
            'If FilesToProcess IsNot Nothing Then

            Dim query = _
                From order In FilesToProcess.AsEnumerable() _
                Where order.Field(Of Integer)("TotalRows") > 1 _
                Select order

            Dim ImagesToDrop As DataTable = query.CopyToDataTable()

            Dim FilesList As String = String.Join("','", (From row In ImagesToDrop.AsEnumerable Select row("FileNamePattern").ToString.ToLower).ToArray)

            Sql = "SELECT DISTINCT lower(table_name) as table_name, drop_image, truncate_table " +
                    "FROM " + ClientName.ToString + "_db.fl2_table_options "

            Using connection As New OdbcConnection(ConnectionString)
                Dim adapter As New OdbcDataAdapter(Sql, connection)

                Try

                    connection.Open()
                    adapter.Fill(TableList)

                Catch ex As OdbcException

                    InsertFileLogging(ClientName, "", "", "", "", "Error in DropTableImages - Get Tables Failed" + ex.Message.ToString, 1)

                    EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error in DropTableImages"
                    EmailBody = " Error in Automated File Loader, Error in DropTableImages - Get Tables Failed : " + "<BR/>" + "<BR/>"
                    EmailBody += ex.Message.ToString

                    Email.SendEMail(EmailSubject, EmailBody, "", "", False, EmailTable)

                    Return False

                End Try

            End Using

            Using MyConn As New OdbcConnection(ConnectionString)
                Dim MyComm As New OdbcCommand
                MyConn.Open()
                MyComm.Connection = MyConn

                For Each row As DataRow In TableList.Rows

                    ForTableName = row("table_name")
                    DropImage = row("drop_image")
                    TruncateTable = row("truncate_table")

                    If DropImage Then
                        Sql = "DROP TABLE IMAGE " + ForTableName.ToString
                    ElseIf TruncateTable Then
                        Sql = "TRUNCATE TABLE " + ForTableName.ToString
                    End If

                    Try

                        MyComm.CommandText = Sql
                        MyComm.ExecuteNonQuery()

                    Catch ex As Exception

                        If ex.Message.Contains("no RAM image") Then
                            Exit Try
                        End If

                        EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error in DropTableImages "
                        EmailBody = "Error in File Loader Dropping Table Images or truncating tables for : " + ForTableName.ToString + Environment.NewLine + Environment.NewLine + _
                                    +" Error in File Loader, please check" + Environment.NewLine + Environment.NewLine + ex.Message.ToString

                        Email.SendEMail(EmailSubject, EmailBody, Directory, Command, False, EmailTable)

                    End Try

                Next

            End Using

        End If

        Return True

    End Function
    Public Function GetClientDetails(ByRef ClientName As String) As DataTable
        Dim Sql As String = ""
        Dim ClientDetails As New DataTable

        Dim builder As New StringBuilder


        builder.Append("Select distinct ")
        builder.Append("lower(case when right(trim(ftp_directory),1) = '\' then trim(ftp_directory) else trim(ftp_directory)||'\' end) as ftp_directory, ")
        builder.Append("first(case when right(trim(zip_file_working_directory),1) = '\' then trim(zip_file_working_directory) else trim(zip_file_working_directory)||'\' end) over(partition by lower(case when right(trim(ftp_directory),1) = '\' then trim(ftp_directory) else trim(ftp_directory)||'\' end) order by zip_file_working_directory desc) as file_working_directory, ")
        builder.Append("first(case when right(trim(wxloader_directory),1) = '\' then trim(wxloader_directory) else trim(wxloader_directory)||'\' end) over(partition by lower(case when right(trim(ftp_directory),1) = '\' then trim(ftp_directory) else trim(ftp_directory)||'\' end) order by wxloader_directory desc) as wxloader_directory, ")
        builder.Append("first(zip_file) over (partition by lower(case when right(trim(ftp_directory),1) = '\' then trim(ftp_directory) else trim(ftp_directory)||'\' end) order by zip_file desc) as zip_files ")
        builder.Append("from ")
        builder.Append(ClientName.ToString)
        builder.Append("_db.fl2_file_feed_details where active = 1; ")


        ' Get internal String value from StringBuilder.
        Sql = builder.ToString

        Using connection As New OdbcConnection(ConnectionString)
            Dim adapter As New OdbcDataAdapter(Sql, connection)

            Try
                connection.Open()
                adapter.Fill(ClientDetails)

            Catch ex As Exception

                InsertFileLogging(ClientName, "", "", "", "", "Error in GetClientDetails" + ex.Message.ToString, 1)

                Dim EmailSubject As String = "ERROR - " + ClientNameFormatted.ToString + " - Error in GetClientDetails"
                Dim EmailBody As String = "An attempt was made to get the client details from the DB - Please see the error below for details : " + Environment.NewLine + Environment.NewLine + _
                    +ex.Message.ToString + Environment.NewLine + Environment.NewLine + Sql.ToString

                Email.SendEMail(EmailSubject, EmailBody, "", "", False, EmailTable)

                Throw New Exception("Error Occured in GetClientDetails: " + ex.Message.ToString)

            End Try

        End Using

        If ClientDetails.Rows.Count = 0 Then

            InsertFileLogging(ClientName, "", "", "", "", "Client Details Missing for Automated File Loader", 1)

            Throw New Exception("Error Occured in GetClientDetails : Client Details Missing")

        End If

        For Each row As DataRow In ClientDetails.Rows

            Dim Dr As DataRow = DirectoriesToProcess.NewRow

            Dr("FTP_Directory") = row("ftp_directory").ToString

            If row("zip_files").ToString = "1" And (row("file_working_directory").ToString = "" Or row("file_working_directory").ToString = "\") Then
                Dr("FileWorkingDirectory") = row("wxloader_directory").ToString
            ElseIf row("zip_files").ToString = "1" Then
                Dr("FileWorkingDirectory") = row("file_working_directory").ToString
            Else
                Dr("FileWorkingDirectory") = ""
            End If

            If row("zip_files").ToString = "1" Then
                Dr("FileDirectory") = row("wxloader_directory").ToString
            Else
                Dr("FileDirectory") = ""
            End If

            Dr("ZipFiles") = row("zip_files").ToString

            DirectoriesToProcess.Rows.Add(Dr)

        Next

        Return DirectoriesToProcess

    End Function

    Public Function MoveZipFileAndExtractData(ByVal FTP_Directory As String, ByVal FileWorkingDirectory As String, ByVal FileDirectory As String, ByVal DevRun As Boolean) As Boolean

        Dim ArchiveDirectory As String = FileDirectory + "Archive\"
        Dim ArchiveZip As String = String.Format(FileDirectory + "Archive\" + DateTime.Now.ToString("yyyyMMdd") + ".zip")

        Try

            Dim Dirs As String = FTP_Directory.ToString

            For Each fi As String In IO.Directory.GetFiles(Dirs, "*", IO.SearchOption.AllDirectories)

                If fi.ToString.ToLower.Contains(".zip") Then

                    Dim FileName As String = ""
                    FileName = fi.ToString.Substring(fi.ToString.LastIndexOf("\") + 1, (fi.ToString.Length - fi.ToString.LastIndexOf("\") - 1))

                    If File.Exists(ArchiveDirectory + FileName) Then
                        File.Delete(ArchiveDirectory + FileName)
                    End If

                    If DevRun Then
                        File.Copy(fi.ToString, ArchiveDirectory + FileName.ToString)
                    Else
                        File.Move(fi.ToString, ArchiveDirectory + FileName.ToString)
                    End If

                    'unzip the files
                    Using archive As ZipArchive = ZipFile.OpenRead(ArchiveDirectory + FileName.ToString)

                        For Each entry As ZipArchiveEntry In archive.Entries

                            If entry.FullName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase) Or entry.FullName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) _
                                Or entry.FullName.EndsWith(".dat", StringComparison.OrdinalIgnoreCase) Or entry.FullName.EndsWith(".xls", StringComparison.OrdinalIgnoreCase) _
                                Or entry.FullName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase) Then

                                If File.Exists(FileWorkingDirectory + entry.Name) Then
                                    File.Delete(FileWorkingDirectory + entry.Name)
                                End If

                                entry.ExtractToFile(Path.Combine(FileWorkingDirectory, entry.Name))

                            End If

                        Next

                    End Using

                    'zip the moved files
                    Try

                        Using archive As ZipArchive = ZipFile.Open(ArchiveZip, ZipArchiveMode.Update)

                            archive.CreateEntryFromFile(ArchiveDirectory + FileName.ToString, FileName)

                        End Using

                    Catch ex As Exception

                        'noramlly out of memory - just try the next file...
                        Continue For

                    End Try

                    File.Delete(ArchiveDirectory + FileName)

                End If

            Next

        Catch ex As Exception

            Throw New Exception("Error Occured in MoveZipFileAndExtractData on the FTP : " + ex.Message.ToString)

        End Try

        Return True

    End Function

    Public Function SendExtractionSummaryEmail(ByVal ExtractFilename As String, ByVal ClientName As String) As Boolean
        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""
        Dim Directory As String = ""
        Dim ExtractData As New DataTable
        Dim ComparisonCounts As New DataTable
        Dim LongDate As String = ExtractFilename.ToString.Replace("Extraction Summary", "").Replace(".txt", "")
        Dim FileDate As Date = Date.ParseExact(LongDate, "yyyyMMddHHmm", CultureInfo.CurrentCulture)
        Dim FileDatePrint As String = FileDate.ToString("yyyy-MM-dd", CultureInfo.CurrentCulture)
        Dim ExtractSql As String = GetExtractSql(FileDatePrint)
        Dim CountsSql As String = GetCountsSql(ExtractFilename.ToString)

        Using connection As New OdbcConnection(ConnectionString)
            Dim adapter As New OdbcDataAdapter(ExtractSql, connection)
            Dim adapter1 As New OdbcDataAdapter(CountsSql, connection)

            Try

                connection.Open()
                adapter.Fill(ExtractData)
                adapter1.Fill(ComparisonCounts)

            Catch ex As OdbcException

                InsertFileLogging(ClientName, "", "", "", "", "Error in SendExtractionSummaryEmail" + ex.Message.ToString, 1)

                EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error in SendExtractionSummaryEmail"
                EmailBody = " Error in Automated File Loader, Error in SendExtractionSummaryEmail : " + "<BR/>" + "<BR/>"
                EmailBody += ex.Message.ToString

                Email.SendEMail(EmailSubject, EmailBody, "", "", False, EmailTable)

                Return False

            End Try

        End Using

        For Each Row As DataRow In ComparisonCounts.Rows
            If Row("result").ToString = "ERROR" Then
                EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Comparision error - File Loader Completed for File Date : " + FileDatePrint.ToString
                Exit For
            Else
                EmailSubject = "SUCCESS - " + ClientNameFormatted.ToString + " - File Loader Completed for File Date : " + FileDatePrint.ToString
            End If
        Next

        EmailBody += "<html>"
        EmailBody = "The daily feed " + FileDatePrint.ToString + " has arrived and has been loaded into the database. <BR/>"
        EmailBody += "Please check the logs to ensure the data has been loaded correctly, a summary of the delta volumes follows: <BR/>"
        EmailBody += "<BR/>"
        EmailBody += "<BR/>"

        Dim html As New System.Text.StringBuilder
        Dim i As Integer
        html.AppendLine("<table border='1' cellpadding='3' cellspacing='5' bgcolor='MintCream' align='left' rules='all'><tr>")
        For Each col As System.Data.DataColumn In ExtractData.Columns
            html.AppendLine("<th bgcolor='MediumBlue'>" & col.Caption & "</th>")
        Next
        html.AppendLine("</tr>")
        For Each dr As System.Data.DataRow In ExtractData.Rows
            html.AppendLine("<tr>")
            For i = 0 To dr.ItemArray.Length - 1
                html.AppendLine("<td>" & dr.ItemArray(i).ToString & "</td>")
            Next
            html.AppendLine("</tr>")
        Next
        html.AppendLine("</table>")
        EmailBody += html.ToString
        EmailBody += "<BR/>"
        EmailBody += "<BR/>"
        EmailBody += "The delta volumes checks post load when compared to the control file are as follows: <BR/>"

        EmailBody += "<BR/>"
        EmailBody += "<BR/>"

        Dim html1 = New System.Text.StringBuilder

        EmailBody += "<BR/>"
        EmailBody += "<BR/>"

        html1.AppendLine("<table border='1' cellpadding='3' cellspacing='5' bgcolor='MintCream' align='left' rules='all'><tr>")
        For Each col As System.Data.DataColumn In ComparisonCounts.Columns
            html1.AppendLine("<th bgcolor='MediumBlue'>" & col.Caption & "</th>")
        Next
        html1.AppendLine("</tr>")
        For Each dr As System.Data.DataRow In ComparisonCounts.Rows
            html1.AppendLine("<tr>")
            For i = 0 To dr.ItemArray.Length - 1
                html1.AppendLine("<td>" & dr.ItemArray(i).ToString & "</td>")
            Next
            html1.AppendLine("</tr>")
        Next
        html1.AppendLine("</table>")

        EmailBody += html1.ToString

        Email.SendEMail(EmailSubject, EmailBody, Directory, "", False, EmailTable)

        Return True

    End Function

    Private Function DataTableToCSVString(table As DataTable) As String
        With New Text.StringBuilder
            Dim once = False

            'headers
            For Each col As DataColumn In table.Columns
                If once = False Then
                    once = True
                Else
                    .Append(",")
                End If
                .Append(col.ColumnName)
            Next

            .AppendLine()

            'rows
            For Each s In table.Select.Select(Function(row) String.Join(",", row.ItemArray))
                .AppendLine(s)
            Next

            Return .ToString
        End With
    End Function

    Public Function CheckFileCount()

        Dim SetDt As New DataTable

        'are there some files sets that we need to check?
        Try
            Sql = " SELECT DISTINCT set_key " + _
                               "FROM   " + ClientName.ToString + "_db.fl2_file_feed_details " + _
                               "WHERE  Active = 1 " + _
                                "AND 	set_key > '' "

            Using connection As New OdbcConnection(ConnectionString)
                Dim adapter As New OdbcDataAdapter(Sql, connection)

                connection.Open()
                adapter.Fill(SetDt)

            End Using

        Catch ex As Exception
            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in CheckFileCount", "Error in Automated File Loader, failed to get active file counts from fl2_file_feed_details" + "<BR/>" + "<BR/>", "", Command, False, EmailTable)

            Return False

        End Try

        If SetDt.Rows.Count = 0 Then
            'there are no active sets so skip further checks
            Return True

        End If

        'check all the files in the set
        For Each Rec As DataRow In SetDt.Rows

            Dim Dt As New DataTable

            Try
                Sql = " SELECT  set_key, file_name " + _
                        "FROM   " + ClientName.ToString + "_db.fl2_file_feed_details " + _
                        "WHERE  Active = 1 " + _
                         "AND 	set_key = '" + Rec.Item("set_key").ToString + "' "

                Using connection As New OdbcConnection(ConnectionString)
                    Dim adapter As New OdbcDataAdapter(Sql, connection)

                    connection.Open()
                    adapter.Fill(Dt)

                End Using

            Catch ex As Exception
                Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in CheckFileCount", "Error in Automated File Loader, failed to get active file counts from fl2_file_feed_details" + "<BR/>" + "<BR/>", "", Command, False, EmailTable)

                Return False

            End Try

            Dt.Columns.Add("number_of_files", GetType(Integer))

            Dim MaxFiles As Integer = 0

            'loop through all the files in the db that are in the set
            For Each Row As DataRow In Dt.Rows
                Dim TotalFiles As Integer = 0
                Dim SearchString As String = "^" + Regex.Replace(Row.Item("file_name").ToString.Replace(".", "\."), "1", "\d").Trim()

                Row.Item("number_of_files") = TotalFiles

                'check all files in the ftp to see if they are the one we are expecting 
                For Each File As DataRow In FilesToProcess.Rows

                    If Regex.Match(File.Item("FileToLoad").ToString.ToLower, SearchString.ToLower).Success Then
                        'if we find a matching file count it
                        TotalFiles += 1
                        Row.Item("number_of_files") = TotalFiles
                        File("FileSet") = Rec.Item("set_key").ToString
                    End If

                Next

                'whatever the maximum file count we have is store that
                If TotalFiles > MaxFiles Then
                    MaxFiles = TotalFiles
                End If

            Next

            'loop back through to check that each file has the same as the max amount of files we are expecting and that none are missing
            For Each Row As DataRow In Dt.Rows
                If Row.Item("number_of_files") < MaxFiles Then

                    Dim Dr As DataRow = MissingFiles.NewRow
                    Dr("FileSet") = Row.Item("set_key").ToString
                    Dr("FileType") = Regex.Replace(Row.Item("file_name").ToString.ToLower.Replace(".csv", "").Replace(".txt", "").Replace(".dat", "").Replace(".xlsx", "").Replace(".xls", "").Replace(".zip", ""), "1", "").Trim()
                    Dr("ReceivedQuantity") = Row.Item("number_of_files").ToString
                    Dr("ExpectedQuantity") = MaxFiles.ToString
                    MissingFiles.Rows.Add(Dr)

                    'remove all the files in the set from the list of files to process so we can continue to process other files
                    For i As Integer = FilesToProcess.Rows.Count - 1 To 0 Step -1
                        If FilesToProcess.Rows(i).Item("FileSet").ToString() = Row.Item("set_key").ToString() Then

                            If FilesToProcess.Rows(i).Item("WorkingDirectoryUsed") = 1 Then
                                'zip files have already been moved to archive, so delete the unzipped files from the working directory regardless of success/fails
                                File.Delete(FilesToProcess.Rows(i).Item("FullFileDirectory"))
                            End If

                            FilesToProcess.Rows.Remove(FilesToProcess.Rows(i))

                        End If
                    Next

                End If
            Next

        Next

        If MissingFiles.Rows.Count > 0 Then
            'something missing...
            Return False
        Else
            'all is good
            Return True
        End If

    End Function

    Public Function FixQuotedFiles(ByVal FileToMove As String, ByVal FileDirectory As String, ByVal EventID As String, ByVal ColumnCount As Integer) As Boolean
        Dim CurrentDirectory As String = ""
        Dim CurrentFile As String = ""
        Dim ArchiveDirectory As String = ""

        Try

            CurrentDirectory = FileToMove.Substring(0, FileToMove.LastIndexOf("\") + 1)
            CurrentFile = FileToMove.Substring(FileToMove.LastIndexOf("\") + 1, (FileToMove.Length - FileToMove.LastIndexOf("\") - 1))
            ArchiveDirectory = String.Format(FileDirectory + "Archive\" + CurrentFile)

            Console.WriteLine("Moving File : " + CurrentFile)

            If File.Exists(CurrentDirectory + CurrentFile) = False Then
                Dim fs As FileStream = File.Create(CurrentDirectory + CurrentFile)
                fs.Close()
            End If

            ' Ensure that the file does not exist. means I can run more than once
            If File.Exists(ArchiveDirectory) Then
                File.Delete(ArchiveDirectory)
            End If

            ' Move the file.
            File.Move(CurrentDirectory + CurrentFile, ArchiveDirectory)

        Catch ex As Exception

            Return False

        End Try

        Try

            'now fix the file
            Dim FileName As String = ArchiveDirectory.ToString
            Dim filenameFixed As String = FileToMove.ToString

            Dim odjReader As New IO.StreamReader(FileName, System.Text.Encoding.GetEncoding("Windows-1252"))
            Dim objWriter As New System.IO.StreamWriter(filenameFixed, True, System.Text.Encoding.GetEncoding("Windows-1252"))


            '  A string to hold each line as it is read
            Dim line As String = String.Empty
            Dim NewLine() As String
            Dim PrintLine As String = ""
            Dim FieldCounter As Integer = 0

            ' Read from the file
            ' As long as there is something left to read
            Do While odjReader.Peek <> -1

                '  Read the next line
                line = odjReader.ReadLine
                ' split on ","
                NewLine = line.Split(",")

                For Each word In NewLine

                    'check if it's a proper field (does it start with a quote),
                    If word.Substring(0, 1) = ControlChars.Quote Then
                        'if it's a proper field then increase the count
                        FieldCounter += 1
                    End If

                    If FieldCounter = 1 Then
                        'start of line
                        'add a quote and the first field
                        PrintLine = ControlChars.Quote
                        PrintLine += word.Replace(ControlChars.Quote, "")

                    ElseIf FieldCounter = ColumnCount Then
                        'end of line
                        FieldCounter = 0

                        'add the seperator, the last field, and the final quote
                        PrintLine += """,""" + word.Replace(ControlChars.Quote, "")
                        PrintLine += ControlChars.Quote

                        ' Add edited text to the output file
                        objWriter.WriteLine(PrintLine)

                    Else
                        'any other field
                        If word.Substring(0, 1) = ControlChars.Quote Then
                            'if it's a proper field add the seperator and the field
                            PrintLine += """,""" + word.Replace(ControlChars.Quote, "")

                        Else
                            'else if it's a dodgy split line then just add the field
                            PrintLine += word.Replace(ControlChars.Quote, "")

                        End If

                    End If

                Next

            Loop

            '  Tidy up when finished
            odjReader.Close()
            odjReader = Nothing

            objWriter.Close()
            objWriter = Nothing

            Return True

        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in FixQuotedFiles", "Error in Automated File Loader, Error fixing file " + "<BR/>" + "<BR/>" + ex.Message.ToString, "", Command, False, EmailTable)

            InsertFileLogging(ClientName, EventID, CurrentDirectory, ArchiveDirectory + FileToMove, "", "Error Fixing File : " + ex.Message.ToString, 1)

            Return False

        End Try


    End Function

    Public Function SaveExcelToCSV(ByVal FileToMove As String, ByVal FileDirectory As String, ByVal EventID As String, ByVal DevRun As Boolean) As Boolean

        Dim CurrentDirectory As String = ""
        Dim CurrentFile As String = ""
        Dim ArchiveDirectory As String = ""

        Try

            CurrentDirectory = FileToMove.Substring(0, FileToMove.LastIndexOf("\") + 1)
            CurrentFile = FileToMove.Substring(FileToMove.LastIndexOf("\") + 1, (FileToMove.Length - FileToMove.LastIndexOf("\") - 1))
            ArchiveDirectory = String.Format(FileDirectory + "Archive\" + CurrentFile)

            Console.WriteLine("Moving File : " + CurrentFile)

            If File.Exists(CurrentDirectory + CurrentFile) = False Then
                Dim fs As FileStream = File.Create(CurrentDirectory + CurrentFile)
                fs.Close()
            End If

            ' Ensure that the file does not exist. means I can run more than once
            If File.Exists(ArchiveDirectory) Then
                File.Delete(ArchiveDirectory)
            End If

            ' Move the file.
            If DevRun Then
                File.Copy(CurrentDirectory + CurrentFile, ArchiveDirectory)
            Else
                File.Move(CurrentDirectory + CurrentFile, ArchiveDirectory)
            End If

        Catch ex As Exception

            Return False

        End Try


        Try

            'now convert the file
            Dim FileName As String = ArchiveDirectory.ToString
            Dim FilenameFixed As String = FileToMove.ToString.Replace(".xlsx", ".txt").Replace(".xls", ".txt")

            Dim strConn As String = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" & FileName.ToString & ";Extended Properties=Excel 12.0;"
            Dim objWriter As New System.IO.StreamWriter(FilenameFixed, True, System.Text.Encoding.GetEncoding("Windows-1252"))

            Dim conn As OleDbConnection = Nothing
            Dim cmd As OleDbCommand = Nothing
            Dim da As OleDbDataAdapter = Nothing
            Dim workSheetName As String = ""
            Dim PrintLine As String = ""

            conn = New OleDbConnection(strConn)
            conn.Open()

            workSheetName = conn.GetSchema("Tables").Rows(0)("TABLE_NAME")

            cmd = New OleDbCommand("SELECT * FROM [" & workSheetName & "]", conn)
            cmd.CommandType = CommandType.Text
            da = New OleDbDataAdapter(cmd)
            Dim dt As DataTable = New DataTable()
            da.Fill(dt)

            'print the headers
            For y As Integer = 0 To dt.Columns.Count - 1

                PrintLine &= dt.Columns(y).ColumnName

                If y < dt.Columns.Count - 1 Then
                    PrintLine &= ControlChars.Tab
                End If

            Next y

            objWriter.WriteLine(PrintLine)

            'print the data
            For x As Integer = 0 To dt.Rows.Count - 1

                PrintLine = ""

                For y As Integer = 0 To dt.Columns.Count - 1

                    PrintLine &= dt.Rows(x)(y).ToString.Replace(ControlChars.Tab, "").Replace(ControlChars.Lf, "").Replace(ControlChars.NewLine, "").Replace(ControlChars.Cr, "")

                    If y < dt.Columns.Count - 1 Then
                        PrintLine &= ControlChars.Tab
                    End If

                Next y

                If Not String.IsNullOrWhiteSpace(PrintLine.ToString.Replace(ControlChars.Tab, "")) Then

                    objWriter.WriteLine(PrintLine)

                End If
            Next x


            '  Tidy up when finished
            If conn.State = ConnectionState.Open Then
                conn.Close()
            End If

            objWriter.Close()
            objWriter = Nothing

            Return True

        Catch ex As Exception

            Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + " - Error in SaveExcelToCSV", "Error in Automated File Loader, Error saving excel to csv file " + "<BR/>" + "<BR/>" + ex.Message.ToString, "", Command, False, EmailTable)

            InsertFileLogging(ClientName, EventID, CurrentDirectory, ArchiveDirectory + FileToMove, "", "Error saving excel to csv file : " + ex.Message.ToString, 1)

            Return False

        End Try


    End Function




    Public Sub SetupDataTables()

        With FileToLoadVariables
            .Columns.Add("file_key")
            .Columns.Add("file_name")
            .Columns.Add("set_key")
            .Columns.Add("zip_file")
            .Columns.Add("staging_table_name")
            .Columns.Add("wxloader_directory")
            .Columns.Add("file_name_date_pattern")
            .Columns.Add("wxloader_statement")
            .Columns.Add("active")
            .Columns.Add("full_path_toload")
            End With

        With UnknownFiles
            .Columns.Add("UnknownFileName")
            End With

        With RejectedRecordsFiles
            .Columns.Add("RejectedFileName")
            .Columns.Add("NumberOfRecords")
            .Columns.Add("LogFileDirectory")
            .Columns.Add("ErrorFileDirectory")
            End With

        With ErroredFiles
            .Columns.Add("ErroredFileName")
            .Columns.Add("LogFileDirectory")
            .Columns.Add("ErrorFileDirectory")
            End With

        With WarehouseErroredFiles
            .Columns.Add("FileName")
            .Columns.Add("Eventid")
            .Columns.Add("Reason")
            End With

        With WarehouseResults
            .Columns.Add("Reason")
            .Columns.Add("Source")
            .Columns.Add("Eventid")
            .Columns.Add("Showid")
            .Columns.Add("TotalNoOfRecords")
            End With

        With MissingFiles
            .Columns.Add("FileSet")
            .Columns.Add("FileType")
            .Columns.Add("ReceivedQuantity")
            .Columns.Add("ExpectedQuantity")
            End With

        With WarehouseValidationResults
            .Columns.Add("FileName")
            .Columns.Add("TableName")
            .Columns.Add("ValidationError")
            .Columns.Add("ValidationResult")
            .Columns.Add("LoadingOfThisFileStopped")
            End With

        With ChangedFiles
            .Columns.Add("FileName")
            .Columns.Add("NoOfColsInFile")
            .Columns.Add("NoOfColsInTable")
            End With

        With EmptyFiles
            .Columns.Add("FileName")
            .Columns.Add("NoOfColsInFile")
            .Columns.Add("NoOfColsInTable")
            End With

        With InActiveFiles
            .Columns.Add("Filename")
            End With

        With DirectoriesToProcess
            .Columns.Add("FTP_Directory", GetType(String))
            .Columns.Add("FileWorkingDirectory", GetType(String))
            .Columns.Add("FileDirectory", GetType(String))
            .Columns.Add("ZipFiles", GetType(String))
            End With

        With FilesToProcess
            .Columns.Add("FileToLoad", GetType(String))
            .Columns.Add("FullFileDirectory", GetType(String))
            .Columns.Add("TotalRows", GetType(Integer))
            .Columns.Add("FileNamePattern", GetType(String))
            .Columns.Add("FileSet", GetType(String))
            .Columns.Add("WorkingDirectoryUsed", GetType(Integer))
            End With

        With SortedFilesToProcess
            .Columns.Add("FileToLoad", GetType(String))
            .Columns.Add("FullFileDirectory", GetType(String))
            .Columns.Add("TotalRows", GetType(Integer))
            .Columns.Add("FileNamePattern", GetType(String))
            End With

    End Sub

    Public Sub InsertStartEndToMasterTable(ByVal Process As String)
        Dim InsertSql As String = ""

        InsertSql = "insert into master_build_log " +
                    "       (           " +
                    "       Client,     " +
                    "       process,    " +
                    "       time_       " +
                    "       )           " +
                    "values             " +
                    "       (           " +
                    "      '" + ClientName.ToString + "', " +
                    "       '" + Process.ToString + "', " +
                    "       current_timestamp " +
                    "       ) "

        Using MyConn As New OdbcConnection(ConnectionString)
            Dim MyComm As New OdbcCommand
            MyConn.Open()
            MyComm.Connection = MyConn

            Try

                MyComm.CommandText = InsertSql
                MyComm.ExecuteNonQuery()

            Catch ex As Exception

                Dim EmailSubject As String = "ERROR - " + ClientNameFormatted.ToString + " - Error in InsertStartEndToMasterTable"
                Dim EmailBody As String = " Error in Automated File Loader, Error in InsertStartEndToMasterTable : " + "<BR/>" + "<BR/>"
                EmailBody += ex.Message.ToString

                Email.SendEMail(EmailSubject, EmailBody, "", "", False, EmailTable)

            End Try

        End Using

    End Sub

End Class
