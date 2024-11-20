use polars::prelude::*;
use std::fs::File;



fn first_query()->Result<DataFrame,PolarsError> {
    
    let file = File::open("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")?;
    let df = CsvReader::new(file)
    .finish()?;
    let filtered_df = df
    .clone()
    .lazy()
    .filter(
        col("Electric Vehicle Type")
        .is_not_null()
        .and(col("City").eq(lit("Seabeck")))

    )
    .select(&[col("City"), col("Electric Vehicle Type")])
    .collect()?;
    Ok(filtered_df)
}

fn second_query()->Result<DataFrame,PolarsError>{
       
    let file = File::open("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")?;
    let df = CsvReader::new(file)
    .finish()?;
    
    let avg_df = df
    .clone()
    .lazy()
    .select([
        col("Electric Range").mean().alias("Average Electric Range")
    ])
    .collect()?;
    Ok(avg_df)
}

fn third_query()->Result<DataFrame,PolarsError>{
    let file = File::open("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")?;
    let df = CsvReader::new(file)
    .finish()?;
    

    let res_df = df.clone().lazy().filter(col("Legislative District").eq(lit(15)))
    .collect()?;
    Ok(res_df)
}
fn fourth_query()-> Result<DataFrame,PolarsError>{
    let file = File::open("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")?;
    let df = CsvReader::new(file)
    .finish()?;

    let high_range_df = df
    .clone()
    .lazy()
    .with_columns([col("Electric Range").max().alias("max_range")])
    .filter(col("Electric Range").eq(col("max_range")))
    .select([col("Make"),col("Model")])
    .collect()?;
    Ok(high_range_df)
}
fn fifth_query()->Result<DataFrame,PolarsError>{
    let file = File::open("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")?;
    let df = CsvReader::new(file)
    .finish()?;

    let county_df =df
    .clone()
    .lazy()
    .group_by([col("County") , col("Model Year") , col("Make")])
    .agg([col("Make").count().alias("Count")])
    .collect()?;
    Ok(county_df)
}
fn sixth_query()->Result<DataFrame,PolarsError>{
    let file = File::open("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")?;
    let df = CsvReader::new(file)
    .finish()?;
    let electric_df = df
    .clone()
    .lazy()
    .group_by([col("Electric Vehicle Type"),col("Electric Utility")])
    .agg([col("Electric Vehicle Type").count().alias("EV Type Count")])
    .collect()?;
    Ok(electric_df)
}
fn seventh_query()->Result<DataFrame,PolarsError>{
    let file = File::open("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")?;
    let df = CsvReader::new(file)
    .finish()?;
    let bd_df =df
    .clone()
    .lazy()
    .group_by([col("Make"),col("Model")])
    .agg([col("Model").count().alias("Vehicle Count")])
    .collect()?;
    Ok(bd_df)
}
fn main(){
    let fq = first_query();
    let sq = second_query();
    let tq = third_query();
    let fq = fourth_query();
    let fiq = fifth_query();
    let siq = sixth_query();
    let seq = seventh_query();
}