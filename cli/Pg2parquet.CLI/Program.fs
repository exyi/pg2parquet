open System.Data
open Argu
open System
open System.Linq
open Npgsql
open Pg2parquet

type ExportArgs =
    | [<AltCommandLine("-o")>] Output of string
    | [<AltCommandLine("-q")>] Query of string
    | [<AltCommandLine("-t")>] Table of string

    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | Output _ -> "The output parquet file"
            | Query _ -> "The sql query to execute, which results will be exported"
            | Table _ -> "The table to export, same as using -q 'select * from <table>'"

type MainArgs =
    | [<AltCommandLine("-c"); Unique; Inherit>] Connection of string
    | [<AltCommandLine("-h"); Unique; Inherit>] Host of string
    | [<AltCommandLine("-U"); Unique; Inherit>] User of string
    | [<AltCommandLine("-d"); Unique; Inherit>] Dbname of string
    | [<AltCommandLine("-p"); Unique; Inherit>] Port of string
    // | [<AltCommandLine("-p")>] Port of string
    | [<CliPrefix(CliPrefix.None)>] Export of ParseResults<ExportArgs>

    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | Connection _ -> "The Npgsql connection string, see https://www.npgsql.org/doc/connection-string-parameters.html"
            | Host _ -> "Specifies the host name of the machine on which the server is running."
            | User _ -> "The user name to connect as."
            | Dbname _ -> "The name of the database to connect to."
            | Port _ -> "Specifies the TCP port or the local Unix-domain socket file extension on which the server is listening for connections. Defaults to the value of the PGPORT environment variable or 5432"
            | Export _ -> "Export query or table into a parquet file"
let connect (args: ParseResults<MainArgs>) =
    let connectionStringFragments =
        args.GetAllResults()
            |> List.choose (fun arg ->
                match arg with
                | Connection s -> Some s
                | Host s -> Some $"Host={s}"
                | User s -> Some $"Username={s}"
                | Dbname s -> Some $"Database={s}"
                | Port s -> Some $"Port={s}"
                | _ -> None)
    let connectionString = String.Join(";", connectionStringFragments)
    try
        try
            let conn = new NpgsqlConnection(connectionString)
            conn.Open()
            conn
        with err when err.Message.Contains "No password has been provided but the backend requires one" ->
            Console.Error.Write("Password: ")
            let password = Console.ReadLine()
            let conn = new NpgsqlConnection(connectionString + ";Password=" + password)
            conn.Open()
            conn
 
    with NpgsqlException as err ->
        Console.Error.WriteLine($"DB connection failed: {err.GetType().Name} {err}")

        exit 2

let errorOut (s: string) =
    Console.Error.WriteLine(s)
    exit 1

[<EntryPoint>]
let main (args: string[]) =
    Console.Error.WriteLine($".net version = {Environment.Version}")
    let parser = ArgumentParser.Create<MainArgs>(programName = "pg2parquet")
    if args.Length = 0 then
        Console.Error.WriteLine (parser.PrintUsage())
        0
    else

    let results: ParseResults<_> =
        try
            parser.Parse(args)
        with e ->
            Console.Error.WriteLine(e.Message)
            exit 1
    NpgsqlTypeHandlers.RegisterTypeHandlers()
    let conn = connect results
    match results.TryGetSubCommand() with
    | None -> results.Raise("No command specified.", ErrorCode.HelpText)
    | Some (Export args) ->
        // let query = results.GetResult(Export Query)
        let query = args.TryGetResult(Query) |> Option.defaultWith (fun () -> "SELECT * FROM " + args.GetResult(Table))
        let output = args.TryGetResult(Output) |> Option.defaultWith (fun () -> errorOut "--output / -o option is required!")
        
        let command = new NpgsqlCommand(query, connection = conn)
        let reader = command.ExecuteReader(CommandBehavior.SchemaOnly)
        let dbColumns = reader.GetColumnSchema()
        reader.Dispose()
        let schemaUtils = SchemaUtils()
        let pqSchema = schemaUtils.GetParquetSchema(dbColumns)
        // b.Read<int>()
        
        for x in dbColumns do
             printfn $"Column {x.ColumnName}: {x.PostgresType.Name} {x.NumericPrecision} {x.PostgresType.Namespace} {x.PostgresType.InternalName} {x.PostgresType.DisplayName} {x.PostgresType.FullName} {x.PostgresType.OID}"
        
        let exporter = Pg2PqCopier.Create(dbColumns, pqSchema)
        // let b = conn.BeginBinaryExport("")
        let nRows = exporter.Copy(output, conn, query).GetAwaiter().GetResult()
        printfn $"Copied %d{nRows} rows"
            
        0
    | _ -> 1

