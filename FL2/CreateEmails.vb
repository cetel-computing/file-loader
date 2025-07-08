Module CreateEmails

    Public Sub SendCompletedEmails()
        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""
        Dim Directory As String = ""
        Dim ErrorFilename As String = ""
        Dim CompleteEmail As Boolean = True

        If UnknownFiles.Rows.Count > 0 Then

            EmailSubject = "WARNING - " + ClientNameFormatted.ToString + " - Unknown Files found during this run."

            EmailBody = ConvertToHtmlFile(UnknownFiles, "These files were moved to the unknown files directory")

            Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

        End If

        If RejectedRecordsFiles.Rows.Count > 0 Then

            EmailSubject = "WARNING - " + ClientNameFormatted.ToString + " - Files had rejections during this run."

            EmailBody = ConvertToHtmlFile(RejectedRecordsFiles, "Please check the links provided to see the rejections...")

            Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

        End If

        If ErroredFiles.Rows.Count > 0 Then

            EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error - Files have errored loading to staging during this run."

            EmailBody = ConvertToHtmlFile(ErroredFiles, "Please check the links provided to see the errors...")

            Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

        End If

        If WarehouseErroredFiles.Rows.Count > 0 Then

            EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error - Files have errored mapping to warehouse during this run."

            EmailBody = ConvertToHtmlFile(WarehouseErroredFiles, "Please check these files. The files have errored mapping to the warehouse")

            Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

        End If

        If ChangedFiles.Rows.Count > 0 Then

            EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error - Files have the wrong format."

            EmailBody = ConvertToHtmlFile(ChangedFiles, "Please check these files. The number of columns in the file does not match the number of columns in the DB table!")

            Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

        End If

        If EmptyFiles.Rows.Count > 0 Then

            EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Error - Empty Files have been sent."

            EmailBody = ConvertToHtmlFile(EmptyFiles, "Please check these files. They appear to be contain no data!")

            Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

        End If

        If WarehouseValidationResults.Rows.Count > 0 Then

            EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " - Warehouse Validation Errors."

            EmailBody = ConvertToHtmlFile(WarehouseValidationResults, "Please check these errors.")

            Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

        End If

        If InActiveFiles.Rows.Count > 0 Then

            EmailSubject = "WARNING - " + ClientNameFormatted.ToString + " - Inactive files have been received."

            EmailBody = ConvertToHtmlFile(InActiveFiles, "These files have been received on the FTP but are set to Inactive on the database. They have been moved to the inactive directory!")

            Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

        End If

        'Complete email - defaults to true, only set to false if carnival load
        If CompleteEmail = True Then

            If FilesToProcess.Rows.Count > 0 Then

                FilesToProcess.Columns.Remove("FileNamePattern")
                FilesToProcess.Columns.Remove("FileSet")
                FilesToProcess.Columns.Remove("WorkingDirectoryUsed")

                EmailSubject = "SUCCESS - " + ClientNameFormatted.ToString + " - Automated File Loader Complete"

                EmailBody = ConvertToHtmlFile(FilesToProcess, "These files were loaded during this run...")

                Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)

            End If

        End If

    End Sub

    Public Sub SendMissingFilesEmail(ByVal NoOfFiles As Integer)
        Dim EmailSubject As String = ""
        Dim EmailBody As String = ""
        Dim Directory As String = ""
        Dim ErrorFilename As String = ""
        Dim ClientNameInitCap As String = StrConv(ClientName.ToString, VbStrConv.ProperCase)

        Events.InsertFileLogging(ClientName, "0", "Automated File Loader", "Automated File Loader", "", "Automated File Loader Terminated : Wrong Number of Files Found : " + DateTime.Now, 0)

        EmailSubject = "ERROR - " + ClientNameFormatted.ToString + " : ERROR - Automated File Loader Terminated : Wrong Number of Files Found @ " + DateTime.Now

        EmailBody = ConvertToHtmlFile(MissingFiles, "These files are missing or have the wrong number of files...")

        Email.SendEMail(EmailSubject, EmailBody, Directory, ErrorFilename, False, EmailTable)


    End Sub

    Public Function ConvertToHtmlFile(ByVal targetTable As DataTable, ByVal EmailDescription As String) As String
        Dim myHtmlFile As String = ""
        Dim myBuilder As System.Text.StringBuilder = New System.Text.StringBuilder()

        'Open tags and write the top portion.
        myBuilder.Append("<html xmlns='http://www.w3.org/1999/xhtml'>")
        myBuilder.Append("<head>")
        myBuilder.Append("<title>")
        myBuilder.Append("Page-")
        myBuilder.Append(Guid.NewGuid().ToString())
        myBuilder.Append("</title>")
        myBuilder.Append("</head>")
        myBuilder.Append(EmailDescription.ToString)
        myBuilder.Append("<BR/>")
        myBuilder.Append("<BR/>")
        myBuilder.Append("<table border='1' cellpadding='5' cellspacing='0' ")
        myBuilder.Append("style='border: solid 1px Black; font-size: small;'>")

        'Build Table
        'Add the headings row.
        myBuilder.Append("<tr align='left' valign='top'>")

        For Each myColumn As DataColumn In targetTable.Columns
            myBuilder.Append("<td align='left' valign='top'>")
            myBuilder.Append(myColumn.ColumnName)
            myBuilder.Append("</td>")
        Next myColumn

        myBuilder.Append("</tr>")

        'Add the data rows.
        For Each myRow As DataRow In targetTable.Rows
            myBuilder.Append("<tr align='left' valign='top'>")

            For Each myColumn As DataColumn In targetTable.Columns
                myBuilder.Append("<td align='left' valign='top'>")
                myBuilder.Append(myRow(myColumn.ColumnName).ToString())
                myBuilder.Append("</td>")
            Next myColumn

            myBuilder.Append("</tr>")
        Next myRow

        'Close tags.
        myBuilder.Append("</table>")
        myBuilder.Append("</body>")
        myBuilder.Append("</html>")

        myHtmlFile = myBuilder.ToString()

        Return myHtmlFile

    End Function

End Module
