#![allow(special_module_name)]
mod args;
mod lib;

use args::Args;
use error::{Error, ProxyResult};
use lib::{downstream_sv1, error, proxy, proxy_config, status, upstream_sv2};
use proxy_config::ProxyConfig;
use roles_logic_sv2::utils::Mutex;

use async_channel::{bounded, unbounded};
use futures::{select, FutureExt};
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use tokio::{sync::broadcast, task};
use v1::server_to_client;

use crate::status::{State, Status};
use tracing::{debug, error, info};

/// 处理命令行参数，如果有的话。
#[allow(clippy::result_large_err)]
fn process_cli_args<'a>() -> ProxyResult<'a, ProxyConfig> {
    let args = match Args::from_args() {
        Ok(cfg) => cfg,
        Err(help) => {
            error!("{}", help);
            return Err(Error::BadCliArgs);
        }
    };
    let config_file = std::fs::read_to_string(args.config_path)?;
    Ok(toml::from_str::<ProxyConfig>(&config_file)?)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let proxy_config = match process_cli_args() {
        Ok(p) => p,
        Err(_) => return,
    };
    info!("PC: {:?}", &proxy_config);

    let (tx_status, rx_status) = unbounded();

    // `tx_sv1_bridge` 发送者用于 `Downstream` 通过 `rx_sv1_downstream` 接收器发送 `DownstreamMessages` 消息给 `Bridge`
    // (Sender<downstream_sv1::DownstreamMessages>, Receiver<downstream_sv1::DownstreamMessages>)
    let (tx_sv1_bridge, rx_sv1_downstream) = unbounded();

    // 发送者/接收者用于从 `Bridge` 向 `Upstream` 发送 SV2 `SubmitSharesExtended` 消息
    // (Sender<SubmitSharesExtended<'static>>, Receiver<SubmitSharesExtended<'static>>)
    let (tx_sv2_submit_shares_ext, rx_sv2_submit_shares_ext) = bounded(10);

    // 发送者/接收者用于从 `Upstream` 向 `Bridge` 发送 SV2 `SetNewPrevHash` 消息
    // (Sender<SetNewPrevHash<'static>>, Receiver<SetNewPrevHash<'static>>)
    let (tx_sv2_set_new_prev_hash, rx_sv2_set_new_prev_hash) = bounded(10);

    // 发送者/接收者用于从 `Upstream` 向 `Bridge` 发送 SV2 `NewExtendedMiningJob` 消息
    // (Sender<NewExtendedMiningJob<'static>>, Receiver<NewExtendedMiningJob<'static>>)
    let (tx_sv2_new_ext_mining_job, rx_sv2_new_ext_mining_job) = bounded(10);

    // 发送者/接收者用于从 `Upstream` 向该 `main` 函数发送新额外随机数，在下游角色连接时传递给 `Downstream`
    // (Sender<ExtendedExtranonce>, Receiver<ExtendedExtranonce>)
    let (tx_sv2_extranonce, rx_sv2_extranonce) = bounded(1);
    let target = Arc::new(Mutex::new(vec![0; 32]));

    // 发送者/接收者用于从 `Bridge` 向 `Downstream` 发送 SV1 `mining.notify` 消息
    let (tx_sv1_notify, _rx_sv1_notify): (
        broadcast::Sender<server_to_client::Notify>,
        broadcast::Receiver<server_to_client::Notify>,
    ) = broadcast::channel(10);

    // 格式化 `Upstream` 连接地址
    let upstream_addr = SocketAddr::new(
        IpAddr::from_str(&proxy_config.upstream_address)
            .expect("解析上游地址失败！"),
        proxy_config.upstream_port,
    );

    let diff_config = Arc::new(Mutex::new(proxy_config.upstream_difficulty_config.clone()));

    // 实例化一个新的 `Upstream`（SV2 池）
    let upstream = match upstream_sv2::Upstream::new(
        upstream_addr,
        proxy_config.upstream_authority_pubkey,
        rx_sv2_submit_shares_ext,
        tx_sv2_set_new_prev_hash,
        tx_sv2_new_ext_mining_job,
        proxy_config.min_extranonce2_size,
        tx_sv2_extranonce,
        status::Sender::Upstream(tx_status.clone()),
        target.clone(),
        diff_config.clone(),
    )
    .await
    {
        Ok(upstream) => upstream,
        Err(e) => {
            error!("创建上游失败: {}", e);
            return;
        }
    };

    // 生成一个任务来执行所有这些初始化工作，以便主线程可以监听状态通道上的信号和故障。这允许 tproxy 在任何这些初始化任务失败时优雅地退出
    task::spawn(async move {
        // 连接到 SV2 上游角色
        match upstream_sv2::Upstream::connect(
            upstream.clone(),
            proxy_config.min_supported_version,
            proxy_config.max_supported_version,
        )
        .await
        {
            Ok(_) => info!("已连接到上游！"),
            Err(e) => {
                error!("连接上游失败，正在退出！: {}", e);
                return;
            }
        }

        // 开始接收来自 SV2 上游角色的消息
        if let Err(e) = upstream_sv2::Upstream::parse_incoming(upstream.clone()) {
            error!("创建 SV2 解析器失败: {}", e);
            return;
        }

        debug!("完成启动上游监听器");
        // 启动任务处理器，在 SV1 下游角色连接后接收提交
        if let Err(e) = upstream_sv2::Upstream::handle_submit(upstream.clone()) {
            error!("创建提交处理器失败: {}", e);
            return;
        }

        // 从上游角色接收扩展额外随机数信息，以便在下游角色连接后发送给下游角色，也用于初始化桥
        let (extended_extranonce, up_id) = rx_sv2_extranonce.recv().await.unwrap();
        info!("debug extended_extranonce:{:?}",extended_extranonce);
        loop {
            let target: [u8; 32] = target.safe_lock(|t| t.clone()).unwrap().try_into().unwrap();
            if target != [0; 32] {
                break;
            };
            async_std::task::sleep(std::time::Duration::from_millis(100)).await;
        }

        info!("主进程上一个哈希:{:?}",rx_sv2_set_new_prev_hash);
        // 实例化一个新的 `Bridge` 并开始处理传入的消息
        let b = proxy::Bridge::new(
            rx_sv1_downstream,
            tx_sv2_submit_shares_ext,
            rx_sv2_set_new_prev_hash,
            rx_sv2_new_ext_mining_job,
            tx_sv1_notify.clone(),
            status::Sender::Bridge(tx_status.clone()),
            extended_extranonce,
            target,
            up_id,
        );
        proxy::Bridge::start(b.clone());

        // 格式化 `Downstream` 连接地址
        let downstream_addr = SocketAddr::new(
            IpAddr::from_str(&proxy_config.downstream_address).unwrap(),
            proxy_config.downstream_port,
        );

        // 接受来自一个或多个 SV1 下游角色（SV1 挖矿设备）的连接
        downstream_sv1::Downstream::accept_connections(
            downstream_addr,
            tx_sv1_bridge,
            tx_sv1_notify,
            status::Sender::DownstreamListener(tx_status.clone()),
            b,
            proxy_config.downstream_difficulty_config,
            diff_config,
        );
    }); // 初始化任务结束

    debug!("启动信号监听器");
    let mut interrupt_signal_future = Box::pin(tokio::signal::ctrl_c().fuse());
    debug!("启动状态监听器");

    // 检查所有任务是否 is_finished() 为 true，如果是，则退出
    loop {
        let task_status = select! {
            task_status = rx_status.recv().fuse() => task_status,
            interrupt_signal = interrupt_signal_future => {
                match interrupt_signal {
                    Ok(()) => {
                        info!("接收到中断信号");
                    },
                    Err(err) => {
                        error!("无法监听中断信号: {}", err);
                        // 在发生错误时也关闭
                    },
                }
                break;
            }
        };
        let task_status: Status = task_status.unwrap();

        match task_status.state {
            // 应仅由下游监听器发送
            State::DownstreamShutdown(err) => {
                error!("关闭来自: {}", err);
                break;
            }
            State::BridgeShutdown(err) => {
                error!("关闭来自: {}", err);
                break;
            }
            State::UpstreamShutdown(err) => {
                error!("关闭来自: {}", err);
                break;
            }
            State::Healthy(msg) => {
                info!("健康消息: {}", msg);
            }
        }
    }
}
