# Pipeline

## Setup

Within Intellij set up an application which points to `pipeline.Driver` and set the VM options to be `-Xms512M -Xmx1024M -Xss1M -XX:+CMSClassUnloadingEnabled`.

## Description

The `Driver.scala` file handles loading source data from `./data`folder. It calls into`Transformer.transform`and stores the result of the transform step in the `result.csv` file.

### Data

`Derms` contain data about the dermatologist who makes evaluations.

`Evaluation` are the evaluations which derms makes for given images (imageId). The diagnosis for an image is given in the Diagnosis field and can either be `NEP` (No Evalution Possible with reasons in the UnfitResons.csv) or some kind of diagnosis (like L.40).

`UnfitReasons` are for evaluations with diagnosis `NEP` which have additional reasons for why the evaluation found the image unfit for evaluation (like being too blurry).

`ClinicalCharacteristics` are used for evaluations where a diagnosis can be done (ie all rows which are not `NEP`). The contain a clinical name and a true/false value indicating if this characteristic is present or not.

## Task

We need an implementation which for each image joins the different derms evaluations along with unfit reasons and clinicals characteristics (depending on the diagnosis).

So for imageId 1 there are two evaluations in the dataset. 

One from Monica with a diagnosis of L.40, which have a some clinical characteristics.
And one from Ionela whth a diagnosis of NEP with a couple of unfit reasons.

The `transform` step should produce one row for each imagine, combining the evaluations of the derms:

```
ImageId, Monica_evaluationId, Monica_diagnosis, Monica_scales, Monica_induration, Ionela_evaluationId, Ionela_diagnosis, Ionela_scales, Ionela_induration, Ionela_bad_light, Ionela_bad_framing
1      , 1                  , L40.0           , true         , false            , 2                  , NEP             , null         , null             , true               , true   
2      , 3                  , L20.2           , false        , false            , 4                  , L41.0           , true         , false            , null               , null        
3      , null               , null            , null         , null             , 5                  , NEP             , null        , null              , true               , true
```


