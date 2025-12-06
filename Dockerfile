# 1. 使用官方 Python 3.13 轻量版作为基础
FROM python:3.13-slim

# 2. 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
# [新增] 设置 pip 默认使用清华源，无需在命令中加 -i
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

# 3. 设置工作目录
WORKDIR /app

# 4. 替换 APT 源为清华源 (Debian Bookworm 专用写法) & 安装系统依赖
# python:3.13-slim 使用的是新的 deb822 格式源文件 (/etc/apt/sources.list.d/debian.sources)
# 这里使用 sed 直接替换域名，安全且兼容性好
RUN sed -i 's/deb.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's|security.debian.org/debian-security|mirrors.tuna.tsinghua.edu.cn/debian-security|g' /etc/apt/sources.list.d/debian.sources && \
    # 开始安装
    apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    wget \
    curl \
    unzip \
    gnupg \
    fonts-liberation \
    libnss3 \
    tzdata \
    && ln -fs /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    # 清理缓存，减小镜像体积
    && rm -rf /var/lib/apt/lists/*

# 5. 复制依赖清单并安装 Python 库
COPY requirements.txt .
# pip 已经通过环境变量配置了清华源，这里直接 install 即可
RUN pip install --no-cache-dir -r requirements.txt

# 6. 复制项目所有代码到容器
COPY . .

# 7. 创建必要的文件夹
RUN mkdir -p logs database configs torrent_downloads

# 8. 暴露端口
EXPOSE 6246

# 9. 启动命令
CMD ["python", "app.py"]