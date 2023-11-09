-- 5. Using your dimensional model, write a SQL query that returns a list of jobs for each company, 
-- ordered and enumerated within each group by the `posted_at` date.

-- Let's suppose that we are using a db that does support the window function row_number().

SELECT  j.company_id,
        c.company_name,
        ROW_NUMBER() OVER (PARTITION BY j.company_id ORDER BY j.posted_at) AS job_rank,
        j.job_id,
        j.posted_at,
        j.location_id,
        j.state,
        j.price
FROM fact_jobs j INNER JOIN dim_companies c
    ON j.company_id = c.company_id
ORDER BY j.company_id,
         job_rank

-- And if we supposed that we are using a db that doesn't support row_number() function, one alternative would be:

SELECT j.company_id,
    c.company_name,
    (
        SELECT COUNT(*)
        FROM fact_jobs j2
        WHERE j2.company_id = j.company_id
        AND j2.posted_at <= j.posted_at
    ) AS job_rank,
    j.job_id,
    j.posted_at,
    j.location_id,
    j.state,
    j.price
FROM fact_jobs j
JOIN dim_companies c ON j.company_id = c.company_id
ORDER BY j.company_id,
        job_rank;

/*6. Discuss how you would obtain and model information (within your schema) 
about the duration of jobs (from `posted` to `expired` states).*/

/* Let's assume that each job goes through a lifecycle with distinct states (posted, expired),
    and that the we have a posted_at column that represents when the job enters the posted state.
    We can create a sort of bridge table that we can call job_state_transitions that would contain these columns: */
        CREATE TABLE job_state_transitions ( 
            job_id INTEGER,
            from_state TEXT, --The state from which the transition occurs (in our case it would be "posted")
            to_state TEXT, --The state to which the transition occurs (in our case it would be "expired")
            transition_time TEXT,  --Timesamp that respresents when the transition has happened
            duration NUMERIC  --The duration of the transition in seconds
            );
    /*
    And to obtain information about the duration of each jobs (from `posted` to `expired` states), the options
    that we can use to insert data in this fact_jobs_duration are:
        - With a python code to calculate this duration
        - With direct SQL queries on our data model
        - Or with a mix of both
    
    Because the data provided in the jobs.json file shows only one state for each job, I am unable to test these options,
    but if we assume that we do have the data needed, and that in the fact_jobs table we would have more than one state per job:*/

    INSERT INTO job_state_transitions (job_id, from_state, to_state, transition_time, duration)
    SELECT
        jp.job_id,
        jp.state AS from_state,
        je.state AS to_state,
        jp.posted_at AS transition_time,
        (je.posted_at - jp.posted_at) AS duration
    FROM
        fact_jobs jp
    INNER JOIN
        fact_jobs je ON jp.job_id = je.job_id AND jp.state = 'posted' AND je.state = 'expired';

