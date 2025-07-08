Public Module StringExtensionMethods

    <System.Runtime.CompilerServices.Extension> _
    Public Function Contains(ByVal str As String, ByVal ParamArray values As String()) As Boolean

        For Each value In values
            If str.Contains(value) Then
                Return True
            End If
        Next

        Return False

    End Function

End Module
