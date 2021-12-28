#### Nigeria Main LTFU, Continue LTFU, and Partner Collection Tool Generation
#### AUTHOR: Randy Yee (PCX5@cdc.gov) and Jeff Naughton (jeffnaughton2@gmail.com)
#### CREATION DATE: 6/14/2021

LTFUtoolFULLGO <- function(){
    #####################
    ## -- Libraries -- ##
    #####################
    library(plyr)
    library(tidyverse)
    library(readxl)
    library(openxlsx)
    library(Hmisc)
    library(data.table)
    library(lubridate)


    ############################
    ## -- Import functions -- ##
    ############################
    source("LTFU ArchiveContinue_JRN.R")
    source("LTFU FileRead_JRN.R")
    source("LTFU Process Functions.R")
    source("LTFU PartnerToolGen_JRN2.R")

    ## Set date values setdatesGO(FROM, TO)
    ## Example: setdatesGO(analysis period start date, data pull date)
    setdatesGO("2021/9/07 00:00:00", "2021/9/20 00:00:00")
    
    # Upload files used in analysis
    prepareGO()

    ##########################################################
    #        combine and mutate LTFU dfs into one            #
    ##########################################################

    df.combine <- dfcombineGO()

    ###########################################
    #######       Create Final LTFU     #######
    #######         Data Frame          #######
    ###########################################

    df_finalGO(df.combine)

    #########################################################
    ################## Continue LTFU check ##################
    #########################################################
    
    contLTFUpathGO("Continue_LTFU_2021-09-06.xlsx", "NEWLTFU_06Sep2021.csv")
    continueltfuGO()
    
    # df.continueLTFU_finaltest2 <- df.continueLTFU_finaltest %>% LTFUcleanGO()
    # df.continueLTFU_finaltest <- df.continueLTFU_final
    
    # datecols <- c("DATA_PULL" ,"ART_START","ART_TIME", "INACTIVE_DATE","INACTIVE_MONTH","INACTIVE_TIME", "LAST_DRUG_PICKUP_INACTIVE")
    # df.continueLTFU_finaltest[,datecols] <- lapply(df.continueLTFU_finaltest[,datecols], function(x) as.character(x))
                                      
    ################################################################
    ################## Main Tool Generator Script ##################
    ################################################################

    ######################## 1) Archive/Continue Determination ######################## 
    ## a) Import New LTFU's & Updated LTFU's and Append
    df_ndr <- ndr_wrangle(df.LTFU_final, # New LTFU File
                          df.continueLTFU_final, # Updated LTFU File
                          data_pull_date
    )

    # Any dupes
    dupe_df <- df_ndr %>% 
      group_by(SITE_PID,FACILITY_UID) %>% 
      filter(n()>1) %>%
      ungroup()

    # Same Inactive Dates
    dupedate_df <- df_ndr %>% 
      group_by(SITE_PID,FACILITY_UID,INACTIVE_DATE) %>% 
      filter(n()>1)

    ## b) Archive Updated LTFU's that have RETURN_VALIDATE "Yes" or Died Date or > 6months
    # (into Folder Historical_LTFU > Stage2)
    ## c) Archive Final Dataset for Next Update (Most recent inactive date taken)
    # (into Folder Continue_LTFU)
    df_cleanNDR <- continue_determine(df_ndr)

    ## d) Import Partner Current Submissions
    # This is already called in LTFU Process functions
    df_partner <- import_partnersubmissions_ALT()

    ## e) Merge Partner Entry to NDR
    df_merged <<- merge_ndrdf_partnersub(df_cleanNDR, df_partner)

    ## In case, partner submission tools are duplicating linelist (df_merged > df_cleanNDR)
    df_final <<- df_merged %>%
      group_by(SITE_PID, FACILITY_UID) %>%
      arrange(desc(INACTIVE_DATE), .by_group = TRUE ) %>%
      slice(1L) %>%
      ungroup()

    dupe_df2 <- df_final %>% 
      group_by(NDR_PID,FACILITY_UID) %>% 
      filter(n()>1)

    ######################## 2) Generate Partner Tools ########################
    ## a) Create Partner Tools
    # (into Folder New Tools)
    generatetools(df_final)

    df_final %>% group_by(IMPLEMENTING_PARTNER,STATE)%>% summarise(n=n())

    #######################################################
    ######## Move LTFU data and Partner submissions #######
    ########          to archive folders            #######
    #######################################################
    
    # moveLTFUdataGO()
    # # 
    # moveSubdataGO()

}

ptm <- proc.time()
LTFUtoolFULLGO()
proc.time() - ptm
