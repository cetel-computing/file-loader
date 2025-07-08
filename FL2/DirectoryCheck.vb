Imports System.IO
Imports System.IO.File
Imports System.Diagnostics
Imports System.Data.SqlClient
Imports System.Text.RegularExpressions

Public Class DirectoryCheck

    Public Shared Function CheckDirectory(ByVal PathToDir As String, Optional ByVal SearchAll As IO.SearchOption = 1) As DataTable

        Try

            Dim Dirs As String = PathToDir.ToString

            For Each filename As String In IO.Directory.GetFiles(Dirs, "*", SearchAll)

                If filename.ToString.ToLower.Contains(".txt") Or filename.ToString.ToLower.Contains(".csv") Or filename.ToString.ToLower.Contains(".dat") Or filename.ToString.ToLower.Contains(".xls") Then

                    Dim foundRow() As DataRow
                    foundRow = FilesToProcess.Select("FullFileDirectory='" + filename.ToString + "'")

                    If foundRow.Count = 0 Then

                        Console.WriteLine(filename)

                        Dim lineCount As String = ""

                        Try
                            lineCount = File.ReadAllLines(filename.ToString).Length

                        Catch ex As Exception
                            If ex.Message.Contains("The process cannot access the file") Then
                                Email.SendEMail("ERROR - " + ClientNameFormatted.ToString + "File Loader Unable to Access File", ex.Message.ToString, "", Command, False, EmailTable)
                                Continue For
                            ElseIf lineCount = "" Then
                                lineCount = GetFileLineCount(filename.ToString).ToString
                            End If
                        End Try

                        Dim Dr As DataRow = FilesToProcess.NewRow

                        Dr("FileToLoad") = filename.Substring(filename.LastIndexOf("\") + 1, (filename.Length - filename.LastIndexOf("\") - 1))
                        Dr("FullFileDirectory") = filename.ToString
                        Dr("TotalRows") = lineCount.ToString

                        Dr("FileNamePattern") = Regex.Replace(filename.Substring(filename.LastIndexOf("\") + 1, (filename.Length - filename.LastIndexOf("\") - 1)).ToString.ToLower.Replace(".csv", "").Replace(".txt", "").Replace(".dat", "").Replace(".xlsx", "").Replace(".xls", ""), "[\d]", "").Trim()
                        
                        If SearchAll = 0 Then
                            Dr("WorkingDirectoryUsed") = 1
                        Else
                            Dr("WorkingDirectoryUsed") = 0
                        End If

                        FilesToProcess.Rows.Add(Dr)

                    End If

                End If

            Next

            Dim view As New DataView(FilesToProcess)
            view.Sort = "FileToLoad ASC"
            SortedFilesToProcess = view.ToTable() '

            Return SortedFilesToProcess

        Catch ex As Exception

            Throw New Exception("Exception Occured in Browse FTP : " + ex.Message.ToString + " - Exception : " + ex.ToString)

        End Try

    End Function

   


    Private Function CompareFileInfos(file1 As FileInfo, file2 As FileInfo) As Integer
        Dim nameDirection As SortOrder = SortOrder.Ascending
        Dim dateDirection As SortOrder = SortOrder.Ascending

        Dim result = 0

        Select Case nameDirection
            Case SortOrder.Ascending
                result = file1.Name.CompareTo(file2.Name)
            Case SortOrder.Descending
                result = file2.Name.CompareTo(file1.Name)
        End Select

        If result = 0 Then
            Select Case dateDirection
                Case SortOrder.Ascending
                    result = file1.LastWriteTime.CompareTo(file2.LastWriteTime)
                Case SortOrder.Descending
                    result = file2.LastWriteTime.CompareTo(file1.LastWriteTime)
            End Select
        End If

        Return result
    End Function

End Class
