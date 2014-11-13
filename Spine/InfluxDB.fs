module InfluxDB

open HttpClient

open System.Collections.Generic
open Newtonsoft.Json

type Points () as this =
    member val name : string = "" with get, set
    member val columns : string seq = Seq.empty with get, set
    member val points : obj seq seq = Seq.empty with get, set

let WritePoints (url:string)
                (database:string)
                (seriesName:string)
                (columns : string seq)
                (points : obj seq seq) =

    let _points = Points()
    _points.name <- seriesName
    _points.columns <- columns
    _points.points <- points

    let serialized = JsonConvert.SerializeObject([_points])

    let url' = sprintf "%s/db/%s/series" url database

    let response = createRequest Post url'
                   |> withBody serialized
                   |> withBasicAuthentication "spine" "spinepassword"
                   |> getResponse
    response

let Connect (url:string)
            (database:string) =
    WritePoints url database

