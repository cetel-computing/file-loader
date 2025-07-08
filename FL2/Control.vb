Imports System.Data.Odbc
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Text
Imports System.Threading
Imports System.Deployment
Imports System.Deployment.Application

Module Control

    Public ConnectionString As String = ""
    Public ClientName As String = ""
    Public ClientNameFormatted As String = ""
    Public ClientUnknownDirectory As String = ""
    Public Sql As String = ""
    Public MyEvents As New Events
    Public UID As String = ""
    Public PWD As String = ""
    Public DSN As String = ""
    Public ActiveFile As Boolean = True
    Public UnknownFile As Boolean = False
    Public EmailTable As New DataTable
    Public UnknownFiles As New DataTable
    Public RejectedRecordsFiles As New DataTable
    Public ErroredFiles As New DataTable
    Public WarehouseErroredFiles As New DataTable
    Public MissingFiles As New DataTable
    Public FileToLoadVariables As New DataTable
    Public WarehouseResults As New DataTable
    Public WarehouseValidationResults As New DataTable
    Public ChangedFiles As New DataTable
    Public EmptyFiles As New DataTable
    Public InActiveFiles As New DataTable
    Public FilesToProcess As New DataTable
    Public DirectoriesToProcess As New DataTable
    Public SortedFilesToProcess As New DataTable


    Sub Main()

        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""
        Dim Directory As String = ""
        Dim ErrorFilename As String = ""
        Dim WarehouseLoadResponse As String = ""
        Dim FileKey As String = ""
        Dim RunAsDev As Boolean = False
        Dim DevServer As String = ""

        Dim CorrectNumFiles As Boolean = False

        Dim arguments As String() = Environment.GetCommandLineArgs()

        ClientName = arguments(1).ToString.ToUpper

        ClientNameFormatted = StrConv(ClientName.ToString, VbStrConv.ProperCase)

        ClientUnknownDirectory = ClientName.ToString

        If arguments.Length > 2 Then
            DevServer = arguments(2).ToString.ToUpper

            'change the client name for dev so emails are less confusing...
            ClientNameFormatted = "***" + DevServer.ToString + "***" + ClientNameFormatted.ToString
            RunAsDev = True
        End If

        ConnectionString = CreateConnectionString(ClientName, DevServer)

        MyEvents.SetupDataTables()

        MyEvents.InsertStartEndToMasterTable("Automated File Loader Started")

        EmailTable = Email.GetEmailList(ClientName)

        Try

            Console.Title = "Automated File Loader"

            DirectoriesToProcess = MyEvents.GetClientDetails(ClientName)

            Events.InsertFileLogging(ClientName, "0", "Automated File Loader for :", ClientName.ToString, "", "Beginning File Load Process for : " + ClientName.ToString, 0)

            For Each DRow As DataRow In DirectoriesToProcess.Rows

                'if we are expecting any zip files then unzip and check the working directory for the unzipped files
                If DRow("ZipFiles") = "1" Then

                    MyEvents.MoveZipFileAndExtractData(DRow("FTP_Directory"), DRow("FileWorkingDirectory"), DRow("FileDirectory"), RunAsDev)

                    FilesToProcess = DirectoryCheck.CheckDirectory(DRow("FileWorkingDirectory"), 0)

                End If

                FilesToProcess = DirectoryCheck.CheckDirectory(DRow("FTP_Directory"))

            Next

            If FilesToProcess.Rows.Count > 0 Then

                CorrectNumFiles = MyEvents.CheckFileCount()
                If CorrectNumFiles = False Then
                    SendMissingFilesEmail(MissingFiles.Rows.Count)
                    'missing file sets have been removed from process list so will be skipped
                End If

                MyEvents.CreateTableImages(ClientName, FilesToProcess)

                For Each row As DataRow In FilesToProcess.Rows

                    FileToLoadVariables = MyEvents.GetDataBaseDetailsForFile(row("FullFileDirectory"), row("FileToLoad"), ClientName, RunAsDev)

                    If FileToLoadVariables.Rows.Count > 0 Then

                        'save excel to txt
                        If row("FileToLoad").ToString.ToLower.EndsWith(".xls") Or row("FileToLoad").ToString.ToLower.EndsWith(".xlsx") Then

                            MyEvents.SaveExcelToCSV(row("FullFileDirectory"), FileToLoadVariables.Rows(0).Item("wxloader_directory").ToString, FileToLoadVariables.Rows(0).Item("file_key").ToString, RunAsDev)

                            'replace xls details with .csv
                            row("FullFileDirectory") = row("FullFileDirectory").ToString.ToLower.Replace(".xlsx", ".txt").Replace(".xls", ".txt")
                            row("FileToLoad") = row("FileToLoad").ToString.ToLower.Replace(".xlsx", ".txt").Replace(".xls", ".txt")

                        End If

                        Dim HeaderCount As Integer = 0

                        HeaderCount = ValidateCSVFile(row("FullFileDirectory"), row("FileToLoad"), FileToLoadVariables.Rows(0).Item("header_delimiter").ToString, FileToLoadVariables.Rows(0).Item("staging_table_name").ToString)

                        If HeaderCount > 0 Then

                            Dim StagingLoaded As Boolean = False
                            StagingLoaded = MyEvents.LoadDataToStaging(FileToLoadVariables, row("FullFileDirectory"), row("FileToLoad"))

                            Dim StagingValidated As Boolean = False

                            If StagingLoaded Then

                                StagingValidated = StagingValidationChecks(ClientName.ToString, FileToLoadVariables.Rows(0).Item("file_key").ToString, FileToLoadVariables.Rows(0).Item("staging_table_name").ToString, row("FullFileDirectory"))

                            End If

                            If StagingValidated Then

                                FileKey = FileToLoadVariables.Rows(0).Item("file_key").ToString

                                Dim FileDate As String = ""
                                Dim FileDatePattern As String = ""
                                Dim CurrentFileDate As Date

                                'need format yyyymmdd from filename
                                FileDatePattern = FileToLoadVariables.Rows(0).Item("file_name_date_pattern").ToString.ToLower.Replace(".csv", "").Replace(".txt", "").Replace(".dat", "").Replace(".xlsx", "").Replace(".xls", "").Replace(".zip", "").Replace("m", "M").Replace("n", "m")
                                FileDate = row("FileToLoad").Substring(row("FileToLoad").ToString.ToLower.Replace(".csv", "").Replace(".txt", "").Replace(".dat", "").Replace(".xlsx", "").Replace(".xls", "").Replace(".zip", "").Length - FileDatePattern.Length, FileDatePattern.Length).ToString

                                If Date.TryParseExact(FileDate, FileDatePattern, Nothing, Globalization.DateTimeStyles.None, CurrentFileDate) Then
                                    CurrentFileDate = Date.ParseExact(FileDate, FileDatePattern, Nothing).ToString("yyyy-MM-dd")
                                Else
                                    CurrentFileDate = Date.Today.ToString("yyyy-MM-dd")
                                End If

                                WarehouseLoadResponse = MyEvents.MapDataToWarehouse(FileKey.ToString, FilesToProcess, CurrentFileDate, row("FileToLoad"), FileToLoadVariables.Rows(0).Item("staging_table_name").ToString)

                                If WarehouseLoadResponse = "True" Then

                                    If Not FileToLoadVariables.Rows(0).Item("file_extension").ToString.Contains("zip") Then

                                        'zip files have already been moved to archive, so just move txt/csv
                                        MyEvents.MoveFiles(row("FullFileDirectory"), FileToLoadVariables.Rows(0).Item("wxloader_directory"), FileToLoadVariables.Rows(0).Item("file_key").ToString, RunAsDev)

                                    End If

                                End If

                            End If

                            If StagingLoaded And (Not StagingValidated Or Not WarehouseLoadResponse = "True") Then

                                'clean up as we havent loaded the file
                                Events.DeleteFileLoadLog(ClientName.ToString, FileToLoadVariables.Rows(0).Item("file_key").ToString, row("FileToLoad"))

                                Dim FailedReason As String = "Failed Validation Checks"

                                If Not WarehouseLoadResponse = "True" Then

                                    FailedReason = WarehouseLoadResponse

                                End If

                                Dim Dr As DataRow = WarehouseErroredFiles.NewRow
                                Dr("FileName") = row("FileToLoad").ToString
                                Dr("Eventid") = FileToLoadVariables.Rows(0).Item("file_key").ToString
                                Dr("Reason") = FailedReason
                                WarehouseErroredFiles.Rows.Add(Dr)

                            End If

                        End If

                    ElseIf ActiveFile = False Then

                        Try

                            Dim Dr As DataRow = InActiveFiles.NewRow
                            Dr("FileName") = row("FileToLoad").ToString
                            InActiveFiles.Rows.Add(Dr)
                            ActiveFile = True

                        Catch ex As Exception
                            ' do something
                        End Try

                    ElseIf UnknownFile = False Then

                        Try

                            Dim Dr As DataRow = ErroredFiles.NewRow
                            Dr("ErroredFileName") = row("FileToLoad").ToString
                            Dr("LogFileDirectory") = ""
                            Dr("ErrorFileDirectory") = ""
                            ErroredFiles.Rows.Add(Dr)

                        Catch ex As Exception
                            ' do something
                        End Try


                    End If

                    If row("WorkingDirectoryUsed") = 1 Then
                        'zip files have already been moved to archive, so delete the unzipped files from the working directory regardless of success/fails
                        File.Delete(row("FullFileDirectory"))
                    End If

                Next

                'delete unknown files from the loaded files data table
                For Each dr As DataRow In UnknownFiles.Rows

                    For Each drFIle As DataRow In FilesToProcess.Rows
                        If drFIle.Item("FileToLoad").ToString().Contains(dr.Item("UnknownFileName").ToString) Then
                            drFIle.Delete()
                            Exit For
                        End If
                    Next

                Next

                'delete errored files from the loaded files data table
                For Each dr As DataRow In ErroredFiles.Rows

                    For Each drFIle As DataRow In FilesToProcess.Rows
                        If drFIle.Item("FileToLoad").ToString().Contains(dr.Item("ErroredFileName").ToString) Then
                            drFIle.Delete()
                            Exit For
                        End If
                    Next

                Next

                'delete warehouse errored files from the loaded files data table
                For Each dr As DataRow In WarehouseErroredFiles.Rows

                    For Each drFIle As DataRow In FilesToProcess.Rows
                        If drFIle.Item("FileToLoad").ToString().Contains(dr.Item("FileName").ToString) Then
                            drFIle.Delete()
                            Exit For
                        End If
                    Next

                Next

                'delete wrong format files from the loaded files data table
                For Each dr As DataRow In ChangedFiles.Rows

                    For Each drFIle As DataRow In FilesToProcess.Rows
                        If drFIle.Item("FileToLoad").ToString().Contains(dr.Item("FileName").ToString) Then
                            drFIle.Delete()
                            Exit For
                        End If
                    Next

                Next

                'delete empty files from the loaded files data table
                For Each dr As DataRow In EmptyFiles.Rows

                    For Each drFIle As DataRow In FilesToProcess.Rows
                        If drFIle.Item("FileToLoad").ToString().Contains(dr.Item("FileName").ToString) Then
                            drFIle.Delete()
                            Exit For
                        End If
                    Next

                Next

                'delete inactive files from the loaded files data table
                For Each dr As DataRow In InActiveFiles.Rows

                    For Each drFIle As DataRow In FilesToProcess.Rows
                        If drFIle.Item("FileToLoad").ToString().Contains(dr.Item("Filename").ToString) Then
                            drFIle.Delete()
                            Exit For
                        End If
                    Next

                Next

                MyEvents.DropTableImages(ClientName, FilesToProcess)

                'Send emails for changed/rejected/errored/Completion

                SendCompletedEmails()

                MyEvents.InsertStartEndToMasterTable("Automated File Loader Completed")

                Events.InsertFileLogging(ClientName, "0", "Automated File Loader", "Automated File Loader", "", "Finished File Load Process : Finished : " + DateTime.Now, 0)

            Else

                MyEvents.InsertStartEndToMasterTable("Automated File Loader Completed")

                Events.InsertFileLogging(ClientName, "0", "Automated File Loader", "Automated File Loader", "", "No Files to Process : Terminating : " + DateTime.Now, 0)

                EmailSubject = "WARNING - " + ClientNameFormatted.ToString + " - Automated File Loader Completed - No Files to Process"
                EmailBody = ClientNameFormatted.ToString + " : File Load Process : No Files to Process : " + DateTime.Now

                Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

            End If



        Catch ex As Exception

            If ex.Message.Contains("Exception Occured in Browse FTP") Then

                MyEvents.InsertStartEndToMasterTable("Automated File Loader Errored")

                EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error in Automated File Loader - Cannot browse FTP"
                EmailBody = " Error in Automated File Loader - Too many people conected to the Web Server : " + Environment.NewLine + Environment.NewLine + ex.Message.ToString + Environment.NewLine + Environment.NewLine + ex.ToString

                Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

            Else

                MyEvents.InsertStartEndToMasterTable("Automated File Loader Errored")

                EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error in Automated File Loader"
                EmailBody = "Error in Automated File Loader - Please see the error below for details : " + Environment.NewLine + Environment.NewLine + ex.Message.ToString + Environment.NewLine + Environment.NewLine + ex.ToString


                Email.SendEMail(EmailSubject, EmailBody, "", "", False, EmailTable)

            End If

            Console.WriteLine(ex.Message)

        End Try

    End Sub

    Private Function CreateConnectionString(ByVal ClientName As String, ByVal DevServer As String) As String
        Dim Sql As String = ""
        Dim MyCommand As New OdbcCommand
        Dim ConnState As String = ""
        Dim LoginDetails As New DataTable

        ConnectionString = ""

        If DevServer = "DEV" Then
            ConnectionString = ""
        End If

        Sql = "SELECT    supplier_id " +
              "         ,supplier_name " +
              "         ,dsn " +
              "         ,user_name " +
              "         ,user_password " +
              "FROM     fl2_login_details " +
              "WHERE    supplier_name = '" + ClientName + "'"

        Using connection As New OdbcConnection(ConnectionString)
            Dim adapter As New OdbcDataAdapter(Sql, connection)

            Try

                connection.Open()
                adapter.Fill(LoginDetails)

            Catch ex As OdbcException

                Events.InsertFileLogging(ClientName, "", "", "", "", "Error in CreateConnectionString" + ex.Message.ToString, 1)

                Throw New Exception("Error Occured in CreateConnectionString: " + ex.Message.ToString)

            End Try

        End Using

        If LoginDetails.Rows.Count = 1 Then

            For Each row As DataRow In LoginDetails.Rows

                UID = row("user_name").ToString()
                PWD = row("user_password").ToString()
                DSN = row("dsn").ToString()

            Next row

        ElseIf LoginDetails.Rows.Count = 0 Then

            Events.InsertFileLogging(ClientName, "", "", "", "", "Login Details Missing for File Loader for : " + ClientName, 1)

            Throw New Exception("Error Occured in GetClientDetails : Client Details Missing for : " + ClientName)

        ElseIf LoginDetails.Rows.Count > 1 Then

            Events.InsertFileLogging(ClientName, "", "", "", "", "Multiple login Details for : " + ClientName, 1)

            Throw New Exception("Error Occured in GetClientDetails : Multiple login Details for : " + ClientName)

        End If

        ConnectionString = "DSN=" + DSN.ToString + ";UID=" + UID.ToString + ";PWD=" + PWD.ToString + ";"

        Return ConnectionString

    End Function

    

End Module
