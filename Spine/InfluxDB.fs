module InfluxDB

open HttpClient

let private ExecuteQuery query =
    let escaped = System.Uri.EscapeUriString query
    let url = "http://178.62.84.55:8086/db/sensorsaurus/series?u=root&p=root&q=" + escaped
    createRequest Get url
    |> getResponseAsync

let SelectStatsDataForAGivenSequenceOfSensors fieldid nodeid sensorid =
    async {
        let query = sprintf """select mean(value) as mean, max(value) as max, last(value) as latest from /^%s\.%s\.%s/i where time > now() - 28d""" fieldid nodeid sensorid
        let! res = ExecuteQuery query
        match res.EntityBody with
        | Some x -> return x
        | None -> failwith "Unexpected lack of body"
                  return ""
    }

let SelectStatsDataForSensorsOnANode fieldid nodeid =
    async {
        let query = sprintf """select mean(value) as mean, max(value) as max, last(value) as latest from /^%s\.%s\..*/i where time > now() - 28d""" fieldid nodeid
        let! res = ExecuteQuery query
        match res.EntityBody with
        | Some x -> return x
        | None -> failwith "Unexpected lack of body"
                  return ""
    }

let SelectReadingsForSensors fieldid nodeid sensorids starttime endtime =
    let whereClause = match starttime, endtime with
                      | Some st, Some et -> sprintf "where time > %s and time < %s" st et
                      | Some st, None -> sprintf "where time > %s" st
                      | None, Some et -> sprintf "where time < %s" et
                      | None, None -> "where time > now() - 14d"
    async {
        match sensorids with
        | [] -> return None
        | [sensorid] -> let query = sprintf """select * from /%s\.%s\.%s/i %s""" fieldid nodeid sensorid whereClause
                        let! res = (ExecuteQuery query)
                        return res.EntityBody
        | sensorList -> let sensors = sensorList |> String.concat "|" |> sprintf "(%s)"
                        let query = sprintf"""select * from /%s\.%s\.%s/i %s""" fieldid nodeid sensors whereClause
                        let! res = (ExecuteQuery query)
                        return res.EntityBody
    }

let SelectReadingsForSensorType fieldid sensorType =
    async {
        let query = sprintf """select * from /^%s\./i where time > now() - 28d and sensortype = '%s'""" fieldid sensorType
        let! res = (ExecuteQuery query)
        return res.EntityBody
    }

type private InfluxSeriesList = FSharp.Data.JsonProvider<"""[{"name":"list_series_result","columns":["time","name"],"points":[[0,"test.testA"],[0,"test.testB"],[0,"test1.testA"],[0,"test1.testB"],[0,"test2"],[0,"testfield.0013A200406B4AA5.3"],[0,"testfield.0013A200406B4AA5.4"],[0,"testfield.testnode.3"],[0,"testfield.testnode.testsensor"],[0,"testseries"]]}]""">

let DeleteNodeSensors fieldid nodeid =
    let deleteInfluxSeries seriesName =
        async {
            let query = sprintf "drop series %s" seriesName
            do! ExecuteQuery query |> Async.Ignore
        }

    async {
        let regex = System.Text.RegularExpressions.Regex(sprintf "%s.%s.\.*" fieldid nodeid)
        let query = "list series"
        let! res = ExecuteQuery query
        res.EntityBody
        |> Option.iter (fun t -> let data = InfluxSeriesList.Load(t)
                                 data.[0].Points
                                 |> Seq.map (fun t -> t.String)
                                 |> Seq.filter (fun t -> regex.IsMatch(t))
                                 |> Seq.iter (fun t -> deleteInfluxSeries t |> Async.Start))

                                 
    }