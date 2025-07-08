Imports System.Data.Odbc
Imports System.IO

Module DataValidation
    Dim Sql As String = ""

    Public Function CsvToDataTable(csvName As String, Optional delimiter As Char = ","c) As DataTable
        Dim dt = New DataTable()
        For Each line In File.ReadLines(csvName)
            If dt.Columns.Count = 0 Then
                For Each part In line.Split({delimiter})
                    dt.Columns.Add(New DataColumn(part))
                Next
            Else
                Dim row = dt.NewRow()
                Dim parts = line.Split({delimiter})
                For i = 0 To parts.Length - 1
                    row(i) = parts(i)
                Next
                dt.Rows.Add(row)
            End If
        Next
        Return dt
    End Function

    Public Function ValidateCSVFile(ByVal FullFileDirectory As String, ByVal Filename As String, ByVal Delimiter As String, ByVal StagingTable As String) As Integer
        Dim TableColumns As New DataTable
        Dim NoOfColsInFile As Integer

        Sql = "SELECT * FROM " + StagingTable.ToString + " WHERE 1 = 0"

        'fudge to get the delimiter to work, won't pass the value needed...
        If Delimiter = """"",""""" Then
            Delimiter = ""","""
        ElseIf Delimiter = "\t" Then
            Delimiter = ControlChars.Tab
        ElseIf Delimiter.Contains("|") Then
            Delimiter = "|"
        ElseIf Delimiter.Contains("¦") Then
            'Delimiter = Chr(&HA6)
            Delimiter = "�"
        End If

            'Get no of csv columns
            Try

                Using reader As StreamReader = New StreamReader(FullFileDirectory.ToString)

                    Dim lineContents() As String = Split(reader.ReadLine(), Delimiter)

                    NoOfColsInFile = lineContents.Length

                End Using

            Catch ex As Exception
                Events.InsertFileLogging(ClientName, "0", "Automated File Loader for :", ClientName.ToString, "", "ValidateCSVFile has failed when counting columns of CSV file : " + Filename.ToString, 1)

            End Try

            'get no of db table columns
            Using connection As New OdbcConnection(ConnectionString)
                Dim adapter As New OdbcDataAdapter(Sql, connection)

                Try

                    connection.Open()
                    adapter.Fill(TableColumns)

                Catch ex As OdbcException

                    'the staging table is missing or kog has gone or something
                    Dim Dr As DataRow = ChangedFiles.NewRow

                    Dr("FileName") = Filename.ToString
                    Dr("NoOfColsInFile") = NoOfColsInFile.ToString
                    Dr("NoOfColsInTable") = "0"

                    ChangedFiles.Rows.Add(Dr)

                    Return 0

                End Try

            End Using

            Dim NoOfColsInTable As Integer = TableColumns.Columns.Count

            If NoOfColsInTable = NoOfColsInFile Then
                Return NoOfColsInFile
            ElseIf NoOfColsInFile = 1 Then
                Try

                    Dim Dr As DataRow = EmptyFiles.NewRow

                    Dr("FileName") = Filename.ToString
                    Dr("NoOfColsInFile") = NoOfColsInFile.ToString
                    Dr("NoOfColsInTable") = NoOfColsInTable.ToString

                    EmptyFiles.Rows.Add(Dr)

                Catch ex As Exception
                    ' do something
                End Try

                Return 0
            Else
                Try

                    Dim Dr As DataRow = ChangedFiles.NewRow

                    Dr("FileName") = Filename.ToString
                    Dr("NoOfColsInFile") = NoOfColsInFile.ToString
                    Dr("NoOfColsInTable") = NoOfColsInTable.ToString

                    ChangedFiles.Rows.Add(Dr)

                Catch ex As Exception
                    ' do something
                End Try

                Return 0
            End If

    End Function

    Public Function StagingValidationChecks(ByVal ClientName As String, ByVal EventID As String, ByVal StagingTable As String, ByVal Filename As String) As Boolean
        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""
        Dim Directory As String = ""

        Dim FileValidationChecks As New DataTable
        Dim FileSql As String = ""

        Dim Validated As Boolean = True

        'get all the validation checks for this file
        FileSql = "SELECT validation_sql " +
                "        ,stop_load " +
                "        ,fail_threshold " +
                "        ,fail_if_less_than " +
                "        ,fail_message " +
                "FROM    " + ClientName.ToString + "_db.fl2_file_feed_validation " +
                "WHERE   LOWER(file_key) = LOWER('" + EventID.ToString + "') AND active = 1 "

        Using connection As New OdbcConnection(ConnectionString)

            Dim adapter As New OdbcDataAdapter(FileSql, connection)

            Try
                connection.Open()
                adapter.Fill(FileValidationChecks)

            Catch ex As OdbcException
                'currently only set up for FV.  If no table just return true
                Return True

            End Try


            'if we have some checks to do then run through them
            For Each row As DataRow In FileValidationChecks.Rows

                Dim Result As Integer = 0
                'Dim FailIfLess As Integer = Convert.ToInt32(row("fail_if_less_than"))
                'Dim FailThreshold As Integer = Convert.ToInt32(row("fail_threshold"))

                Dim cmd As New OdbcCommand

                Try
                    cmd.Connection = connection
                    cmd.CommandText = row("validation_sql")
                    Dim DBResult = cmd.ExecuteScalar()
                    If Not IsDBNull(DBResult) Then
                        Result = Convert.ToInt32(DBResult)
                    End If
                Catch ex As Exception
                    'if the check sql doesnt work then email and die
                    EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Validation Check Failed"
                    EmailBody = "I'm afraid the validation check has failed, please see error..." + Environment.NewLine + Environment.NewLine + ex.Message.ToString
                    Directory = ""

                    Email.SendEMail(EmailSubject, EmailBody, Directory, Command, False, EmailTable)

                    Return False

                End Try

                'do something to test the result
                If (row("fail_if_less_than") = 1 AndAlso Result < row("fail_threshold")) Or (row("fail_if_less_than") = 0 AndAlso Result > row("fail_threshold")) Then
                    'add the results to a table to be emailed at the end of all the tests

                    Dim Dr As DataRow = WarehouseValidationResults.NewRow

                    Dr("FileName") = Filename.ToString
                    Dr("TableName") = StagingTable.ToString
                    Dr("ValidationError") = row("fail_message").ToString
                    Dr("ValidationResult") = Result.ToString
                    Dr("LoadingOfThisFileStopped") = row("stop_load").ToString

                    WarehouseValidationResults.Rows.Add(Dr)

                    'only stop db load if this flag is set on the db, otherwise continue but all results will be emailed at the end
                    If row("stop_load") = 1 Then
                        'continue all checks, but store that a check failed
                        Validated = False
                    End If

                End If

            Next

        End Using

        If Validated = False Then
            'if anything failed a check the return false
            Return False
        Else
            'otherwise everything is good, or there are no queries to run, or there are queries but they are not marked as stop load
            Return True
        End If

    End Function

    Public Function GetFileLineCount(ByVal FileName As String) As Integer
        Dim total As Integer = 0

        If File.Exists(FileName) Then
            Dim buffer(32 * 1024) As Char
            Dim i As Integer
            Dim read As Integer

            Dim reader As TextReader = File.OpenText(FileName)
            read = reader.Read(buffer, 0, buffer.Length)

            While (read > 0)
                i = 0
                While i < read

                    If buffer(i) = Chr(10) Then
                        total += 1
                    End If

                    i += 1
                End While

                read = reader.Read(buffer, 0, buffer.Length)
            End While

            reader.Close()
            reader = Nothing

            If Not buffer(i - 1) = Chr(10) Then
                total += 1
            End If

        End If

        Return total
    End Function

End Module
