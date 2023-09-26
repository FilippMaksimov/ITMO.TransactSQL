USE Practice
GO
CREATE PROCEDURE NashaBaza.apf_Sync
@dtEndDate date = NULL
AS
BEGIN
BEGIN TRY
	DECLARE @dtReportDate date,
			@checkTableSourse int,
			@checkOurTable int
 
	SET @dtReportDate =(SELECT TOP(1) dtReportDate FROM NashaBaza.OurTable ORDER BY PropId DESC)
	SET @checkTableSourse = (SELECT CHECKSUM(
		CAST(nPokazatelId AS int), dtEndDate, CAST(SUBSTRING(vTerritoryId,1,2) AS int),
		CAST(SUBSTRING(vTerritoryId,4,4) AS int), CAST(nValue AS decimal(6,4)))
		FROM Istochnik.TableSource
		WHERE LineId = (SELECT TOP(1) LineId FROM Istochnik.TableSource ORDER BY LineId DESC))
	SET @checkOurTable = (SELECT CHECKSUM(nCanonId, dtReportDate, nTerOtdelenie, nTerPodrazdel, vProcent) 
		FROM NashaBaza.OurTable
		WHERE PropId = (SELECT TOP(1) PropId FROM NashaBaza.OurTable ORDER BY PropId DESC))

	SET @dtEndDate = ISNULL(@dtEndDate, (SELECT TOP(1) dtEndDate FROM Istochnik.TableSource ORDER BY LineId DESC))

	IF @dtEndDate IS NOT NULL AND @dtEndDate < (SELECT MAX(dtEndDate) FROM Istochnik.TableSource)
		BEGIN
		SET @dtReportDate = @dtEndDate
		SET @checkTableSourse = (SELECT CHECKSUM(
			CAST(nPokazatelId AS int), dtEndDate, CAST(SUBSTRING(vTerritoryId,1,2) AS int),
			CAST(SUBSTRING(vTerritoryId,4,4) AS int), CAST(nValue AS decimal(6,4)))
			FROM Istochnik.TableSource
			WHERE LineId = (SELECT TOP(1) LineId FROM Istochnik.TableSource WHERE dtEndDate = @dtEndDate ORDER BY LineId DESC))
		SET @checkOurTable = (SELECT CHECKSUM(nCanonId, dtReportDate, nTerOtdelenie, nTerPodrazdel, vProcent) 
		FROM NashaBaza.OurTable
		WHERE PropId = (SELECT TOP(1) PropId FROM NashaBaza.OurTable WHERE dtReportDate = @dtEndDate ORDER BY PropId DESC))
		END

	IF @dtEndDate > (SELECT MAX(dtEndDate) FROM Istochnik.TableSource)
		BEGIN
		SET @dtEndDate = 'ERROR'
		END

	IF @dtEndDate = @dtReportDate AND @checkTableSourse = @checkOurTable
		BEGIN
		PRINT 'Data for the last date is synchronized'
		END
	ELSE
		BEGIN
		IF @dtEndDate != @dtReportDate AND @checkTableSourse!= @checkOurTable
			BEGIN
			INSERT INTO NashaBaza.OurTable(nCanonId, dtReportDate, nTerOtdelenie, nTerPodrazdel, vProcent)
			SELECT CAST(nPokazatelId AS int), dtEndDate, CAST(SUBSTRING(vTerritoryId,1,2) AS int),
				CAST(SUBSTRING(vTerritoryId,4,4) AS int), CAST(nValue AS decimal(6,4))
			FROM Istochnik.TableSource
			WHERE LineID = (SELECT TOP(1) LineId FROM Istochnik.TableSource ORDER BY LineId DESC)
			PRINT 'INSERT command was initiated'
			END
		 ELSE
			BEGIN
			IF @dtEndDate = @dtReportDate AND @checkTableSourse != @checkOurTable
				BEGIN
				UPDATE NashaBaza.OurTable
				SET nCanonId = (SELECT CAST(nPokazatelId AS int) FROM Istochnik.TableSource
					WHERE LineId =(SELECT TOP(1) LineId FROM Istochnik.TableSource
						WHERE dtEndDate = @dtEndDate 
						ORDER BY LineId DESC )),
					nTerOtdelenie = (SELECT CAST(SUBSTRING(vTerritoryId,1,2) AS int) FROM Istochnik.TableSource 
					WHERE LineId = (SELECT TOP(1) LineId FROM Istochnik.TableSource
						WHERE dtEndDate=@dtEndDate
						ORDER BY LineId DESC)),			
					nTerPodrazdel = (SELECT CAST(SUBSTRING(vTerritoryId,4,4) AS int) FROM Istochnik.TableSource 
					WHERE LineId = (SELECT TOP(1) LineId FROM Istochnik.TableSource
						WHERE dtEndDate = @dtEndDate
						ORDER BY LineId DESC)),
					vProcent = (SELECT CAST(nValue AS decimal(6,4)) FROM Istochnik.TableSource 
					WHERE LineId = (SELECT TOP(1) LineId FROM Istochnik.TableSource 
						WHERE dtEndDate = @dtEndDate
						ORDER BY LineId DESC))
				WHERE PropId = (SELECT TOP(1) PropId FROM NashaBaza.OurTable 
					WHERE dtReportDate = @dtReportDate
					ORDER BY PropId DESC)
				PRINT 'UPDATE command was initiated'
				END
			ELSE
				BEGIN
				PRINT 'Data for the last date is synchronized'
				END
			END
		END
END TRY 
BEGIN CATCH
	SELECT
		ERROR_LINE() AS ErrorLineNo,
		ERROR_NUMBER() AS ErrorNumber,
		ERROR_PROCEDURE() AS ErrorProcedure,
		ERROR_MESSAGE() AS ErrorMessage,
		'INVALID TYPE OR DATE' AS Discription
	PRINT 'This date is not exists! Please, enter the actual date'
END CATCH 
END
GO

--ДРУГИЕ ВАРИАНТЫ РЕШЕНИЯ
--Можно использовать еще следующую реализацию:
--Инструкция для INSERT. Все недостающие даты будут перенесены OurTable
USE Practice
GO
INSERT INTO NashaBaza.OurTable (nCanonId, dtReportDate, nTerOtdelenie, nTerPodrazdel, vProcent)
	SELECT CAST(i.nPokazatelId AS int), i.dtEndDate, CAST(SUBSTRING(i.vTerritoryId,1,2) AS int),
		CAST(SUBSTRING(i.vTerritoryId,4,4) AS int), CAST(i.nValue AS decimal(6,4))
			FROM Istochnik.TableSource AS i
				LEFT JOIN NashaBaza.OurTable AS n
					ON i.nValue = n.vProcent AND i.dtEndDate = n.dtReportDate 
		WHERE n.vProcent is NULL

--Инструкция для UPDATE 
USE Practice
GO

DECLARE @tableCS1 table (KontSum int NOT NULL, nPokazatelId int NOT NULL, EndDate date not NULL, 
						Otdelenie int NOT NULL, Podrazdelenie int NOT NULL, initValue decimal (6,4) NOT NULL)
INSERT INTO @tableCS1 
SELECT CHECKSUM(CAST(nPokazatelId AS int), dtEndDate, CAST(SUBSTRING(vTerritoryId,1,2) AS int),
			CAST(SUBSTRING(vTerritoryId,4,4) AS int), CAST(nValue AS decimal(6,4))),
	CAST(nPokazatelId AS int), dtEndDate, CAST(SUBSTRING(vTerritoryId,1,2) AS int),
	CAST(SUBSTRING(vTerritoryId,4,4) AS int), CAST(nValue AS decimal(6,4))
	FROM Istochnik.TableSource

DECLARE @tableCS2 table (KontSumEnd int NOT NULL, CanonId int NOT NULL, dtReportDate date NOT NULL,
						nTerOtdelenie int NOT NULL, nTerPodrazdel int NOT NULL, vProcent decimal(6,4) NOT NULL)
INSERT INTO @tableCS2
SELECT CHECKSUM(nCanonId, dtReportDate, nTerOtdelenie, nTerPodrazdel, vProcent),
	nCanonId, dtReportDate, nTerOtdelenie, nTerPodrazdel, vProcent
	FROM NashaBaza.OurTable

SELECT *
FROM @tableCS1 AS i
	JOIN @tableCS2 r
		ON i.nPokazatelId = r.CanonId AND i.EndDate = r.dtReportDate AND i.Otdelenie = r.nTerOtdelenie 
			AND i.Podrazdelenie = r.nTerPodrazdel AND i.initValue = r.vProcent
WHERE KontSum != KontSumEnd

UPDATE NashaBaza.OurTable
SET nCanonId = (SELECT i.nPokazatelId FROM @tableCS1 AS i
		JOIN @tableCS2 r ON i.nPokazatelId = r.CanonId AND i.EndDate = r.dtReportDate AND i.Otdelenie = r.nTerOtdelenie 
			AND i.Podrazdelenie = r.nTerPodrazdel AND i.initValue = r.vProcent
				WHERE EndDate = @dtEndDate AND KontSum != KontSumEnd),
	nTerOtdelenie = (SELECT i.Otdelenie FROM @tableCS1 AS i
		JOIN @tableCS2 r ON i.nPokazatelId = r.CanonId AND i.EndDate = r.dtReportDate AND i.Otdelenie = r.nTerOtdelenie 
			AND i.Podrazdelenie = r.nTerPodrazdel AND i.initValue = r.vProcent
				WHERE EndDate = @dtEndDate AND KontSum != KontSumEnd),			
	nTerPodrazdel = (SELECT i.Podrazdelenie FROM @tableCS1 AS i
		JOIN @tableCS2 r ON i.nPokazatelId = r.CanonId AND i.EndDate = r.dtReportDate AND i.Otdelenie = r.nTerOtdelenie 
			AND i.Podrazdelenie = r.nTerPodrazdel AND i.initValue = r.vProcent
				WHERE EndDate = @dtEndDate AND KontSum != KontSumEnd),
	vProcent = (SELECT i.initValue FROM @tableCS1 AS i
		JOIN @tableCS2 r ON i.nPokazatelId = r.CanonId AND i.EndDate = r.dtReportDate AND i.Otdelenie = r.nTerOtdelenie 
			AND i.Podrazdelenie = r.nTerPodrazdel AND i.initValue = r.vProcent
				WHERE EndDate = @dtEndDate AND KontSum != KontSumEnd)
WHERE PropId = (SELECT TOP(1) PropId FROM NashaBaza.OurTable 
	WHERE dtReportDate = @dtReportDate
		ORDER BY PropId DESC)

-- SELECT (для пятницы)

USE Practice
GO
SELECT i.nPokazatelId, i.dtEndDate, i.nValue, n.vProcent, n.nTerPodrazdel
FROM Istochnik.TableSource AS i
	LEFT JOIN NashaBaza.OurTable AS n
		ON i.nValue = n.vProcent AND i.dtEndDate = n.dtReportDate 
WHERE n.vProcent is NULL
