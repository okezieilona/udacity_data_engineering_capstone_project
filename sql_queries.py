import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')


#Temp Table Scripts
airport_tmp = ( 
    """
    select 
        ident
        ,type
        ,name
        --,elevation_ft
        --,continent
        ,iso_country
        --,iso_region
        ,municipality
        --,gps_code
        ,iata_code
        --,local_code
        ,cast(RTRIM(LTRIM(longitude)) as decimal(16,4)) as longitude
        ,cast(RTRIM(LTRIM(latitude)) as decimal(16,4)) as latitude
        ,state_code
    from airport_table
    where type != 'closed' 
    and iso_country = 'US'
    order by state_code
    
    """)

city_tmp = (
    """
    select e.City
            ,e.State
            ,cc.city_code
            ,e.Median_Age
            ,cast(e.Male_Population as int) as Male_Population
            ,cast(e.Female_Population as int) as Female_Population
            ,cast(e.Total_Population as int) as Total_Population
            ,cast(e.Number_of_Veterans as int) as Number_of_Veterans
            ,cast(e.Foreign_born as int) as Foreign_born
            ,e.Average_Household_Size
            ,e.State_Code
            ,r.*
            
    from (select 
            City
            ,State
            ,city_state_code
            ,avg(Median_Age) as Median_Age
            ,sum(Male_Population) as Male_Population
            ,sum(Female_Population) as Female_Population
            ,sum(Total_Population) as Total_Population
            ,sum(Number_of_Veterans) as Number_of_Veterans
            ,sum(Foreign_born) as Foreign_born
            ,avg(Average_Household_Size) as Average_Household_Size
            ,State_Code
            --,r.*
            --,Race
            --,Count
            from cities
    group by city_state_code, City, State, State_Code ) as e
    inner join race_table r on e.city_state_code = r.city_state_code
    left join city_code cc on lower(e.city_state_code) = lower(cc.city_state_code)
    
    """ 
    )


immigration_tmp = (
    """
    select  cast(cicid as int) as cicid
            ,cast(i94yr as int) as i94yr
            ,cast(i94mon as int) as i94mon
            ,cast(i94cit as int) as i94cit
            ,cast(i94res as int) as i94res
            ,i94port
            ,format_datetime(cast(arrdate as int)) as arrival_date
            ,cast(i94mode as int) as i94mode
            ,i94addr
            ,format_datetime(cast(depdate as int)) as depature_date
            ,cast(i94bir as int) as i94bir
            ,cast(i94visa as int) as i94visa
            --,cast(count as int) as count
            --,to_date(cast(UNIX_TIMESTAMP(dtadfile, 'MMddyyyy') AS TIMESTAMP)) AS dtadfile
            ,visapost
            --,occup
            --,entdepa
            --,entdepd
            --,entdepu
            --,matflag
            ,cast(biryear as int) as biryear
            --,to_date(cast(UNIX_TIMESTAMP(dtaddto, 'MMddyyyy') AS TIMESTAMP)) AS dtaddto
            ,gender
            --,insnum
            ,airline
            --,cast(admnum as decimal(16,8)) as admnum
            --,fltno
            ,visatype
            
    From immigration_data
    
    """ )


avg_temp_tmp = (
        """
        select  month(dt) as month 
                ,cast(avg(AverageTemperature) as decimal(16,4)) as AverageTemperatureOnMonth
                ,cast(avg(AverageTemperatureUncertainty) as decimal(16,4)) as AverageTemperatureUncertaintyOnMonth
                --,City
                ,c.city_code
                --,Country
                
        From temp_table t
        left join city_code c on lower(t.City) = lower(c.city_name)
        where dt >= '2010-01-01' 
              and Country = 'United States'
        group by month(dt)
                ,c.city_code
                --,City
                --,Country
        
        """
    )


row_count_tmp = (
        """
        select table_name,
                sum(source_count) as source_count,
                sum(destination_count) as destination_count
        from (
        select 'city_table' as table_name,
                count(*) as source_count,
                0 as destination_count
        from (select distinct City as city from city_source_table)
        
        union
        
        select 'city_table' as table_name,
                0 as source_count,
                count(city) as destination_count
        from (select distinct city as city from city_dest_table)
        
        
        ----
        union
        
        select 'visa_table' as table_name,
                count(visa_type_id) as source_count,
                0 as destination_count
        from visa_source_table
        
        union
        
        select 'visa_table' as table_name,
                0 as source_count,
                count(visa_type_id) as destination_count
        from visa_dest_table
        
        
        ----
        
        union
        
        select 'airport_table' as table_name,
                count(iata_code) as source_count,
                0 as destination_count
        from airport_source_table
        where type != 'closed' 
              and iso_country = 'US'
        
        union
        
        select 'airport_table' as table_name,
                0 as source_count,
                count(iata_code) as destination_count
        from airport_dest_table
        
        
        ----
        union
        
        select 'country_table' as table_name,
                count(country_code) as source_count,
                0 as destination_count
        from country_dest_table
        
        union
        
        select 'country_table' as table_name,
                0 as source_count,
                count(country_code) as destination_count
        from country_dest_table
       
        
        ----
        union
        
        select 'transport_table' as table_name,
                count(trans_mode_id) as source_count,
                0 as destination_count
        from transport_source_table
        
        union
        
        select 'transport_table' as table_name,
                0 as source_count,
                count(trans_mode_id) as destination_count
        from transport_dest_table
        
        
        ----
         union
        
        select 'immigration_table' as table_name,
                count(cicid) as source_count,
                0 as destination_count
        from immigration_source_table
        
        union
        
        select 'immigration_table' as table_name,
                0 as source_count,
                count(cicid) as destination_count
        from immigration_dest_table
        )
        group by table_name
        
        """    
    )

duplicate_record_check = (
        """
        select 'city_table' as table_name,
                count(*) as number_of_duplicates
        from (select city
        from city_dest_table
        group by city having count(city) > 1)
        
        union
        
        select 'airport_table' as table_name,
                count(*) as number_of_duplicates
        from (select iata_code
        from airport_dest_table
        group by iata_code having count(iata_code) > 1)
        
        
        """
    )


