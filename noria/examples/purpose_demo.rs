use noria::ControllerHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Product (pid varchar(255), name varchar(255), \
                                    category varchar(255), PRIMARY KEY(pid));
               CREATE TABLE Purchase (username varchar(255), pid varchar(255), \
                                    purchase_id varchar(255), PRIMARY KEY(pid));  
               CREATE TABLE Visit (username varchar(255), pid varchar(255), \
                                    visit_id varchar(255), PRIMARY KEY(pid));                  
 
               # queryable materialized view
               QUERY ADS_PurchaseCategory: \
                            SELECT Purchase.username, Product.category, COUNT(purchase_id) AS count \
                            FROM Product, Purchase \
                            WHERE Product.pid = Purchase.pid AND Purchase.username = ? \
                            GROUP BY Purchase.username, Product.category;
               QUERY ADS_VisitCategory: \
                            SELECT Visit.username, Product.category, COUNT(visit_id) AS count \
                            FROM Product, Visit \
                            WHERE Product.pid = Visit.pid AND Visit.username = ? \
                            GROUP BY Visit.username, Product.category;
                            ";
    // let aid = 1;

    let mut srv = ControllerHandle::from_zk("127.0.0.1:2181/purpose")
        .await
        .unwrap();
    srv.install_recipe(sql).await.unwrap();
    let g = srv.graphviz().await.unwrap();
    println!("{}", g);

    // let mut awvc = srv.view("ArticleWithVoteCount").await.unwrap();
    // println!("Creating article...");
    // let article = awvc.lookup(&[aid.into()], true).await.unwrap();
    // if article.is_empty() {
    //     println!("Creating new article...");
    //     let title = "test title";
    //     let url = "http://pdos.csail.mit.edu";
    //     let mut articles = srv.table("Article").await.unwrap();
    //     articles
    //         .insert(vec![aid.into(), title.into(), url.into()])
    //         .await
    //         .unwrap();
    // }

    // let mut vote = srv.table("Vote").await.unwrap();
    // // Then create a new vote:
    // println!("Casting vote...");
    // let uid = SystemTime::now()
    //     .duration_since(UNIX_EPOCH)
    //     .unwrap()
    //     .as_secs() as i64;

    // // Double-voting has no effect on final count due to DISTINCT
    // vote.insert(vec![aid.into(), uid.into()]).await.unwrap();
    // vote.insert(vec![aid.into(), uid.into()]).await.unwrap();

    // println!("Finished writing! Let's wait for things to propagate...");
    // tokio::timer::delay(Instant::now() + Duration::from_millis(1000)).await;

    // println!("Reading...");
    // let article = awvc.lookup(&[aid.into()], true).await.unwrap();
    // println!("{:#?}", article);
}
