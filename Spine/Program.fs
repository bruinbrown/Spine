// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open Akka.Actor
open Nancy.Hosting.Self
open System
open Spine.ApiConfiguration
open Spine.Actors
open Spine.Model
open Akka.FSharp

[<EntryPoint>]
let main argv = 

    let system = Spine.ActorSystem.System

    let testField = spawn system "testfield" FieldActor

    System.Threading.Thread.Sleep 10000

    use host = new NancyHost(new Uri("http://localhost:8888"))
    host.Start()

    printfn "Started the web service"
    System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite) |> ignore
    0 // return an integer exit code
