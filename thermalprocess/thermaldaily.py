SELECT 
    SUM(total_energy_difference) AS cumulative_energy
FROM (
    SELECT 
        MVPPolling.mvpnum,
        SUM(energy_difference) AS total_energy_difference
    FROM (
        SELECT 
            mvpnum,
            acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference
        FROM MVPPolling
        WHERE 
            polledTime BETWEEN DATE_SUB(NOW(), INTERVAL 60 MINUTE) AND NOW()
            AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')
    ) MVPPolling
    WHERE 
        energy_difference IS NOT NULL
    GROUP BY 
        MVPPolling.mvpnum
) latest_energy_consumption;