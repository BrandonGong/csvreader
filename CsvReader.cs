using System.Data;
using System.Text;

namespace csvreader;

public class CsvReader : IDisposable
{
    private StreamReader? _stream;
    private CsvReaderOptions _options;

    #region  Constructors
    private CsvReader()
    {
        _options ??= new();
    }
    public CsvReader(Stream stream)
    : this()
    {
        _stream = new(stream);
    }
    public CsvReader(Stream stream, CsvReaderOptions options)
    : this()
    {
        _stream = new(stream);
        _options = options;
    }
    public CsvReader(string path)
    : this()
    {
        _stream = new(File.OpenRead(path));
    }
    public CsvReader(string path, CsvReaderOptions options)
    : this()
    {
        _stream = new(File.OpenRead(path));
        _options = options;
    }
    #endregion

    #region Public
    public string[]? ReadLine()
    {
        ObjectDisposedException.ThrowIf(_stream == null, this);

        if (_stream.EndOfStream) return null;
        var line = new List<string>();
        var value = new StringBuilder();
        bool inQuotes = false;
        char[] c = new char[1];
        Span<char> buffer = new(c);
        while (_stream.Read(buffer) > 0)
        {
            switch (buffer[0])
            {
                case ',':
                    if (inQuotes)
                    {
                        value.Append(buffer[0]);
                    }
                    else
                    {
                        // Add the value of the current entry to the line and clear the buffer
                        line.Add(value.ToString());
                        value.Clear();
                    }
                    break;
                case '\\':
                    // Escape character, add next character to the buffer
                    if (_stream.Read(buffer) > 0)
                        value.Append(buffer[0]);
                    break;
                case '"':
                    inQuotes = !inQuotes;
                    break;
                case '\r' or '\n':
                    if (inQuotes)
                    {
                        value.Append(buffer[0]);
                    }
                    else if (value.Length > 0)
                    {
                        line.Add(value.ToString());
                        return line.ToArray();
                    }
                    break;
                default:
                    value.Append(buffer[0]);
                    break;
            }
        }
        line.Add(value.ToString());
        return line.ToArray();
    }

    public DataTable ReadAsDataTable()
    {
        var dt = new DataTable();
        DataColumn c;
        DataRow r;
        // Get first line of file to determine column count if no header is provided
        string[]? line = ReadLine();
        if (line == null) return dt;
        string[] columnNames;
        if (_options.HasHeaders)
        {
            columnNames = line;
            line = ReadLine();
            if (line == null) return dt;
        }
        else
        {
            columnNames = new string[line.Length];
            for (int i = 0; i < line.Length; i++)
            {
                columnNames[i] = i.ToString();
            }
        }
        foreach (var columnName in columnNames)
        {
            c = new()
            {
                DataType = System.Type.GetType("System.String"),
                ColumnName = columnName,
                ReadOnly = false,
                Unique = false
            };
            dt.Columns.Add(c);
        }
        do
        {
            r = dt.NewRow();
            for (int i = 0; i < line.Length; i++)
            {
                r[columnNames[i]] = line[i];
            }
            dt.Rows.Add(r);
        } while ((line = ReadLine()) != null);
        return dt;
    }
    #endregion

    #region  Private
    #endregion

    #region IDisposable
    public void Dispose()
    {
        _stream?.Dispose();
        _stream = null;
    }
    #endregion 
}
