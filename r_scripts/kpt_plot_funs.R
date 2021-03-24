
give.mean_minutes_kg <- function(x){
  return(c(y =mean(x)+4, label = round(mean(x),digits=2)))}
give.mean_minutes_LPG <- function(x){
  return(c(y =mean(x)+10, label = round(mean(x),digits=1)))}
give.mean_minutes_energia <- function(x){
  return(c(y =mean(x)+70, label = round(mean(x),digits=1)))}
#To make box and whiskers quantiles rather than IQRs.
f <- function(x) {
  r <- quantile(x, probs = c(0.05, 0.25, 0.5, 0.75, 0.95))
  names(r) <- c("ymin", "lower", "middle", "upper", "ymax")
  r }
give.n <- function(x){return(c(y = 0, label = length(x)))}

#Box plot of KPT results, by two groups, and by stove type.
kpt_plot_twovar <- function(df, x_var, y_var , facet_var1 , facet_var2 , plot_title, xlabel,ylabel,giver){
  ggplot(df,aes_string(x=x_var, y = y_var)) +
    stat_summary(fun.data = f, geom="boxplot") +   
    geom_jitter(height = 0,width = 0.1,alpha = 0.25) +
    stat_summary(fun.y=mean, colour="blue", geom="point", shape=18, size=4.5,alpha = 0.5)+
    stat_summary(fun.data = give.mean_minutes_kg, geom = "text",colour="blue") + 
    labs(title= plot_title,y=ylabel,x=xlabel) + stat_summary(fun.data = give.n, geom = "text") + 
    scale_x_discrete(labels = function(x) str_wrap(x, width = 10))+
    theme(axis.text.x = element_text(angle = 60, hjust = 1,size=8), axis.title=element_text(size=9)) + 
    facet_wrap(as.formula(paste("~",facet_var1,"+",facet_var2)),scales = "free",ncol=4,
               labeller = labeller(facet_var = label_wrap_gen(width = 15)))
}
#________________________________________________________

#________________________________________________________

#Box plot of KPT results, by region, and community, and by stove type.  With T-tests.
kpt_plot_ttest <- function(df, x_var, y_var , facet_var1 , facet_var2 , plot_title, xlabel,ylabel,giver){
  ggplot(df,aes_string(x=x_var, y = y_var)) +
    stat_summary(fun.data = f, geom="boxplot") +   
    geom_jitter(height = 0,width = 0.1,alpha = 0.25) +
    stat_summary(fun.y=mean, colour="blue", geom="point", shape=18, size=4.5,alpha = 0.5)+
    # stat_summary(fun.data = giver, geom = "text",colour="blue") + 
    # labs(y=ylabel,x=xlabel) + stat_summary(fun.data = give.n, geom = "text") + 
    # scale_x_discrete(labels = function(x) str_wrap(x, width = 10))+
    # theme(axis.text.x = element_text(angle = 60, hjust = 1)) + 
    facet_grid(as.formula(paste("~",facet_var1,"+",facet_var2)),scales = "free",rows=2,
               labeller = labeller(facet_var = label_wrap_gen(width = 15))) +
    stat_compare_means(method = "t.test") 
}
#________________________________________________________

#________________________________________________________

#Box plot of KPT results, by ONE groups, and by stove type.
kpt_plot_onevar <- function(df, x_var, y_var , facet_var1, plot_title, xlabel,ylabel,giver){
  ggplot(df,aes_string(x=x_var, y = y_var)) +
    stat_summary(fun.data = f, geom="boxplot") +   
    geom_jitter(height = 0,width = 0.1,alpha = 0.25) +
    stat_summary(fun.y=mean, colour="blue", geom="point", shape=18, size=4.5,alpha = 0.5)+
    stat_summary(fun.data = giver, geom = "text",colour="blue") + 
    labs(title= plot_title,y=ylabel,x=xlabel) + stat_summary(fun.data = give.n, geom = "text") + 
    scale_x_discrete(labels = function(x) str_wrap(x, width = 10))+
    theme(axis.text.x = element_text(angle = 60, hjust = 1,size=8),axis.title=element_text(size=9)) + 
    # stat_compare_means(method = "t.test") 
    facet_wrap(as.formula(paste("~",facet_var1)),scales = "fixed",ncol = 4,
               labeller = labeller(facet_var = label_wrap_gen(width = 15)))
}


#Box plot of KPT results, by ONE groups, and by stove type.
kpt_plot_group <- function(df, x_var, y_var, plot_title, xlabel,ylabel,giver){
  ggplot(df,aes_string(x=x_var, y = y_var)) +
    stat_summary(fun.data = f, geom="boxplot") +   
    geom_jitter(height = 0,width = 0.1,alpha = 0.25) +
    stat_summary(fun.y=mean, colour="blue", geom="point", shape=18, size=4.5,alpha = 0.5)+
    stat_summary(fun.data = giver, geom = "text",colour="blue") + 
    labs(title= plot_title,y=ylabel,x=xlabel) + stat_summary(fun.data = give.n, geom = "text") + 
    scale_x_discrete(labels = function(x) str_wrap(x, width = 10))+
    theme(axis.text.x = element_text(angle = 60, hjust = 1,size=8),axis.title=element_text(size=9)) #+ 
    # stat_compare_means(method = "t.test") 
    # facet_wrap(as.formula(paste("~",facet_var1)),scales = "fixed",ncol = 3,
               # labeller = labeller(facet_var = label_wrap_gen(width = 15)))
}
#________________________________________________________

#________________________________________________________

#Box plot of KPT results, by ONE groups, and by stove type.
kpt_plot_onevar_ttest <- function(df, x_var, y_var , facet_var1, plot_title, xlabel,ylabel,giver){
  ggplot(df,aes_string(x=x_var, y = y_var)) +
    stat_compare_means(method = "t.test") +
    stat_summary(fun.data = f, geom="boxplot") +   
    geom_jitter(height = 0,width = 0.1,alpha = 0.25) +
    stat_summary(fun.y=mean, colour="blue", geom="point", shape=18, size=4.5,alpha = 0.5)+
    stat_summary(fun.data = giver, geom = "text",colour="blue") + 
    labs(title= plot_title,y=ylabel,x=xlabel) + stat_summary(fun.data = give.n, geom = "text") + 
    scale_x_discrete(labels = function(x) str_wrap(x, width = 10))+
    theme(axis.text.x = element_text(angle = 60, hjust = 1)) + 
    facet_grid(as.formula(paste("~",facet_var1)),scales = "free",labeller = labeller(facet_var = label_wrap_gen(width = 15)))
}
#________________________________________________________

