Imports System.Net.Mail
Imports System.Xml
Imports System.Linq
Imports System.Data.Odbc

Public Class Email

    Public Shared Function SendEMail(ByVal EmailSubject As String, ByVal EmailBody As String, ByVal Directory As String, ByVal Filename As String, ByVal AttachFiles As Boolean, EmailList As DataTable)

        Try

            Dim mail As New MailMessage()
            Dim SmtpServer As New SmtpClient("192.168.16.8")

            mail.From = New MailAddress("FileLoader@")
            
            For Each row As DataRow In EmailList.Rows
                If row.Item("email_address").ToString.ToLower.Contains("builds@") AndAlso EmailSubject.Contains("ERROR") Then
                    mail.[To].Add("errors@")
                Else
                    mail.[To].Add(row.Item("email_address"))
                End If
            Next row
            
            mail.Subject = EmailSubject
            mail.Body = EmailBody
            mail.IsBodyHtml = True

            If AttachFiles = True Then

                Dim SaveAsFilePath As String = Directory.ToString
                Dim MailAttach As Attachment = New Attachment(SaveAsFilePath)
                mail.Attachments.Add(MailAttach)

                Try

                    SaveAsFilePath = Directory.ToString.Substring(0, Directory.Length - 3) + "err"
                    MailAttach = New Attachment(SaveAsFilePath)
                    mail.Attachments.Add(MailAttach)

                Catch ex As Exception

                End Try

            End If

            SmtpServer.Port = 25
            SmtpServer.Send(mail)

            Return 1

        Catch ex As Exception

            Throw New System.Exception(ex.Message.ToString)

        End Try

    End Function

    Public Shared Function GetEmailList(ByVal ClientName As String) As DataTable
        Dim Emails As New DataTable
        Dim EmailAddress As String = ""

        Sql = "SELECT DISTINCT email_address " +
                "FROM " + ClientName.ToString + "_db.fl2_file_loader_emails"

        Using connection As New OdbcConnection(ConnectionString)
            Dim adapter As New OdbcDataAdapter(Sql, connection)

            Try

                connection.Open()
                adapter.Fill(Emails)

            Catch ex As OdbcException

                Events.InsertFileLogging(ClientName, "", "", "", "", "Error Get Emails - Get EmailList Failed" + ex.Message.ToString, 1)

            End Try

        End Using

        Return Emails

    End Function

End Class
