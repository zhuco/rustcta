FROM rust:1.88

# 安装交叉编译工具
RUN apt-get update && apt-get install -y \
    gcc-x86-64-linux-gnu \
    libc6-dev-amd64-cross \
    && rm -rf /var/lib/apt/lists/*

# 添加目标平台
RUN rustup target add x86_64-unknown-linux-gnu

# 设置交叉编译环境变量
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
ENV CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
ENV CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++

WORKDIR /app
COPY . .

# 编译
RUN cargo build --target x86_64-unknown-linux-gnu --release