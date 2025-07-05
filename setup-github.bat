@echo off
chcp 65001 >nul
echo ========================================
echo 🚀 GitHub仓库设置脚本
echo ========================================
echo.
echo 此脚本将帮助你:
echo 1. 初始化Git仓库
echo 2. 添加所有文件
echo 3. 创建初始提交
echo 4. 连接到GitHub远程仓库
echo 5. 推送代码并触发自动构建
echo.

:: 检查Git是否安装
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Git未安装
    echo 请先安装Git: https://git-scm.com/download/win
    pause
    exit /b 1
)

echo ✅ Git已安装
echo.

:: 获取GitHub仓库信息
set /p GITHUB_USERNAME=请输入你的GitHub用户名: 
set /p REPO_NAME=请输入仓库名称 (默认: rustcta): 
if "%REPO_NAME%"=="" set REPO_NAME=rustcta

echo.
echo 📋 配置信息:
echo GitHub用户名: %GITHUB_USERNAME%
echo 仓库名称: %REPO_NAME%
echo 仓库地址: https://github.com/%GITHUB_USERNAME%/%REPO_NAME%
echo.
set /p CONFIRM=确认以上信息正确吗? (y/n): 
if /i not "%CONFIRM%"=="y" (
    echo 已取消操作
    pause
    exit /b 0
)

echo.
echo 🔧 开始设置Git仓库...

:: 初始化Git仓库
if not exist ".git" (
    echo 初始化Git仓库...
    git init
    if %errorlevel% neq 0 (
        echo ❌ Git初始化失败
        pause
        exit /b 1
    )
    echo ✅ Git仓库初始化完成
) else (
    echo ✅ Git仓库已存在
)

:: 设置Git配置
echo 设置Git配置...
set /p GIT_EMAIL=请输入你的Git邮箱: 
set /p GIT_NAME=请输入你的Git用户名: 

git config user.email "%GIT_EMAIL%"
git config user.name "%GIT_NAME%"
echo ✅ Git配置完成

:: 创建.gitignore中排除的示例配置文件
echo 创建示例配置文件...
if not exist "config\api.yml" (
    copy "config\api.yml.example" "config\api.yml" >nul
    echo ✅ 已创建 config/api.yml (请填入真实API信息)
echo.
echo ⚠️  重要提醒:
echo 1. 请编辑 config/api.yml 文件，填入你的真实API密钥
echo 2. 该文件已被.gitignore排除，不会上传到GitHub
echo 3. 建议先使用测试网进行测试
echo.
)

:: 添加所有文件
echo 添加文件到Git...
git add .
if %errorlevel% neq 0 (
    echo ❌ 添加文件失败
    pause
    exit /b 1
)
echo ✅ 文件添加完成

:: 创建初始提交
echo 创建初始提交...
git commit -m "🎉 Initial commit: RustCTA量化交易系统

✨ 功能特性:
- 🔥 高性能Rust量化交易系统
- 🌐 支持Binance交易所API
- 📊 网格交易策略实现
- 🤖 GitHub Actions自动化CI/CD
- 🐳 Docker容器化部署支持
- 📝 完整的配置和文档

🚀 快速开始:
1. 下载预编译版本
2. 配置API密钥
3. 启动交易程序

📖 详细文档请查看 README.md"

if %errorlevel% neq 0 (
    echo ❌ 创建提交失败
    pause
    exit /b 1
)
echo ✅ 初始提交创建完成

:: 添加远程仓库
echo 添加GitHub远程仓库...
git remote remove origin >nul 2>&1
git remote add origin https://github.com/%GITHUB_USERNAME%/%REPO_NAME%.git
if %errorlevel% neq 0 (
    echo ❌ 添加远程仓库失败
    echo 请确保GitHub仓库已创建: https://github.com/%GITHUB_USERNAME%/%REPO_NAME%
    pause
    exit /b 1
)
echo ✅ 远程仓库添加完成

:: 推送到GitHub
echo 推送代码到GitHub...
echo 注意: 如果是第一次推送，可能需要输入GitHub用户名和密码/Token
git push -u origin main
if %errorlevel% neq 0 (
    echo ❌ 推送失败，尝试使用master分支...
    git branch -M main
    git push -u origin main
    if %errorlevel% neq 0 (
        echo ❌ 推送仍然失败
        echo 可能的原因:
        echo 1. GitHub仓库不存在
        echo 2. 认证失败 (需要Personal Access Token)
        echo 3. 网络连接问题
        echo.
        echo 手动操作步骤:
        echo 1. 在GitHub创建仓库: https://github.com/new
        echo 2. 仓库名称: %REPO_NAME%
        echo 3. 设置为公开仓库
        echo 4. 不要初始化README (我们已经有了)
        echo 5. 重新运行此脚本
        pause
        exit /b 1
    )
)

echo ✅ 代码推送成功!
echo.
echo 🎉 GitHub仓库设置完成!
echo ========================================
echo.
echo 📋 仓库信息:
echo 🔗 仓库地址: https://github.com/%GITHUB_USERNAME%/%REPO_NAME%
echo 🤖 Actions页面: https://github.com/%GITHUB_USERNAME%/%REPO_NAME%/actions
echo 📦 Releases页面: https://github.com/%GITHUB_USERNAME%/%REPO_NAME%/releases
echo.
echo 🚀 接下来的步骤:
echo 1. 访问仓库地址查看代码
echo 2. 等待GitHub Actions自动构建 (约5-10分钟)
echo 3. 构建完成后在Releases页面下载程序
echo 4. 按照README.md说明部署到服务器
echo.
echo 💡 创建Release版本:
echo 要触发自动发布，请创建一个版本标签:
echo git tag v1.0.0
echo git push origin v1.0.0
echo.
echo 📖 更多信息请查看:
echo - README.md: 项目介绍和使用说明
echo - DEPLOYMENT.md: 自动化部署指南 (构建后生成)
echo - 稳定部署方案.md: 多种部署方案对比
echo.
echo ⚠️  重要提醒:
echo 1. 请妥善保管你的API密钥
echo 2. 建议先在测试网环境测试
echo 3. 定期检查GitHub Actions构建状态
echo 4. 关注项目更新和安全补丁
echo.
pause